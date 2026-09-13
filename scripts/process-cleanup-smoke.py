#!/usr/bin/env python3
"""Exercise the local MCP binary on Linux; no service/deployment changes."""

import argparse
import json
import os
from pathlib import Path
import queue
import shlex
import subprocess
import sys
import tempfile
import threading
import time


# Every fixture exits on a private stop file or after 15 seconds, even if the
# smoke runner fails. No process-name search or signalling unrelated PIDs.
FIXTURE = r"""
import json, os, pathlib, signal, sys, time
mode, directory = sys.argv[1:]
directory = pathlib.Path(directory)
signal.alarm(15)
pid = os.fork()
if pid == 0:
    signal.alarm(15)
    if mode == 'detached':
        os.setsid()
    stat = pathlib.Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()
    (directory / 'child.tmp').write_text(json.dumps([os.getpid(), stat[19]]))
    (directory / 'child.tmp').replace(directory / 'child.json')
    print('child-ready', flush=True)
    if mode == 'delayed':
        time.sleep(0.15)
        print('delayed-output', flush=True)
    else:
        while not (directory / 'stop').exists():
            time.sleep(0.02)
    os._exit(0)
while not (directory / 'child.json').exists():
    time.sleep(0.005)
print('root-ready', flush=True)
if mode == 'ordinary':
    os.waitpid(pid, 0)
"""


class MCP:
    def __init__(self, binary, workspace, stderr):
        config = workspace / 'config.json'
        config.write_text(json.dumps({
            'version': 1, 'workspace': str(workspace), 'shell': '/bin/bash',
            'output_store_dir': str(workspace / 'outputs'),
            'child_env': {'inherit': [], 'rules': []},
        }))
        self.process = subprocess.Popen(
            [str(binary), '--config', str(config)], env={},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr,
            text=True,
        )
        self.messages = queue.Queue()
        self.request_id = 0
        threading.Thread(target=self.read_messages, daemon=True).start()

    def read_messages(self):
        try:
            for line in self.process.stdout:
                self.messages.put(json.loads(line))
        except Exception as error:
            self.messages.put(error)
        finally:
            self.messages.put(EOFError('MCP stdout closed'))

    def send(self, message):
        self.process.stdin.write(json.dumps({'jsonrpc': '2.0', **message}) + '\n')
        self.process.stdin.flush()

    def request(self, method, params):
        self.request_id += 1
        self.send({'id': self.request_id, 'method': method, 'params': params})
        deadline = time.monotonic() + 8
        while True:
            message = self.messages.get(timeout=max(0, deadline - time.monotonic()))
            if isinstance(message, Exception):
                raise message
            if message.get('id') != self.request_id:
                continue
            assert 'error' not in message, message
            return message['result']

    def tool(self, name, **arguments):
        result = self.request('tools/call', {'name': name, 'arguments': arguments})
        assert not result.get('isError'), result
        return result['structuredContent']

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=2)
        self.process.stdout.close()


def running(directory):
    pid, birth = json.loads((directory / 'child.json').read_text())
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return fields[19] == birth and fields[0] not in ('Z', 'X')
    except FileNotFoundError:
        return False


def wait_until(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'fixture condition timed out'
        time.sleep(0.02)


def start(client, workspace, mode):
    directory = workspace / mode
    directory.mkdir()
    command = 'exec ' + shlex.join([sys.executable, '-c', FIXTURE, mode, str(directory)])
    response = client.tool('exec_command', cmd=command, yield_time_ms=300)
    wait_until(lambda: (directory / 'child.json').exists())
    return directory, response


def finish(client, response):
    output = response['output']
    deadline = time.monotonic() + 6
    while response.get('session_id'):
        assert time.monotonic() < deadline, 'execution did not finish within 6s'
        response = client.tool('write_stdin', session_id=response['session_id'],
                               chars='')
        output += response['output']
    return response, output


def smoke(client, workspace):
    client.request('initialize', {
        'protocolVersion': '2025-06-18', 'capabilities': {},
        'clientInfo': {'name': 'process-cleanup-smoke', 'version': '1'},
    })
    client.send({'method': 'notifications/initialized'})
    peer = client.tool('start_session', cmd='exec /bin/bash --noprofile --norc',
                       tty=False)['session_id']

    for mode in ('ordinary', 'delayed', 'held-pipe', 'detached'):
        started = time.monotonic()
        directory, response = start(client, workspace, mode)
        if mode == 'ordinary':
            assert response.get('session_id'), response
            response = client.tool('write_stdin', session_id=response['session_id'],
                                   chars='\x03')
        response, output = finish(client, response)
        elapsed = time.monotonic() - started
        assert elapsed < 8, (mode, elapsed)
        if mode == 'ordinary':
            assert response['exit_code'] == 130, response
            wait_until(lambda: not running(directory))
            detail = 'child stopped after Ctrl-C'
        elif mode == 'delayed':
            assert response['exit_code'] == 0, response
            assert 'delayed-output' in output, output
            assert not response.get('capture_error'), response
            assert response.get('output_ref', {}).get('capture_status', 'complete') == 'complete', response
            detail = 'output after root exit captured'
        else:
            assert response['exit_code'] == 0, response
            ref = response['output_ref']
            assert ref['capture_status'] == 'incomplete', response
            assert 'drain deadline' in ref['incomplete_reason'], response
            assert b'child-ready' in Path(ref['path']).read_bytes(), response
            if mode == 'held-pipe':
                wait_until(lambda: not running(directory))
                detail = 'drain deadline, raw retained, child stopped'
            else:
                detail = f'drain deadline, raw retained, detached child running={running(directory)} (allowed)'
        # An active round trip proves that another execution still works.
        peer_response = client.tool('write_stdin', session_id=peer,
                                    chars=f'printf "peer-{mode}\\n"\n')
        assert f'peer-{mode}' in peer_response['output'], peer_response
        assert peer_response.get('session_id') == peer, peer_response
        (directory / 'stop').touch()
        wait_until(lambda: not running(directory))
        print(f'PASS {mode}: {detail}; peer intact ({elapsed:.2f}s)', flush=True)

    response = client.tool('write_stdin', session_id=peer, chars='exit\n')
    response, _ = finish(client, response)
    assert response['exit_code'] == 0, response


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=Path('target/debug/chatgpt-exec-mcp'))
    args = parser.parse_args()
    if sys.platform != 'linux':
        parser.error('this smoke uses Linux /proc and fork/setsid')
    binary = args.binary.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix='exec-cleanup-smoke-') as name:
        workspace = Path(name)
        with (workspace / 'server.stderr').open('w+') as stderr:
            client = MCP(binary, workspace, stderr)
            try:
                smoke(client, workspace)
            except BaseException:
                stderr.seek(0)
                print(stderr.read(), file=sys.stderr)
                raise
            finally:
                try:
                    for directory in workspace.iterdir():
                        if (directory / 'child.json').exists():
                            (directory / 'stop').touch()
                            wait_until(lambda: not running(directory))
                finally:
                    client.close()


if __name__ == '__main__':
    main()

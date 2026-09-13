#!/usr/bin/env python3
"""Build a standalone release binary with builder paths remapped out of diagnostics."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def build(destination):
    source = Path(__file__).resolve().parent.parent
    if destination.exists():
        raise ValueError('output must not exist')
    environment = os.environ.copy()
    for name in ['RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS']:
        if name in environment:
            raise ValueError(f'{name} must be absent for this fixed build recipe')
    cargo_home = Path(environment.get('CARGO_HOME', str(Path.home() / '.cargo'))).resolve(strict=True)
    environment['CARGO_ENCODED_RUSTFLAGS'] = '\x1f'.join([
        f'--remap-path-prefix={Path.home()}=/builder',
        f'--remap-path-prefix={cargo_home}=/cargo',
        f'--remap-path-prefix={source}=/src/chatgpt-exec-mcp',
    ])
    with tempfile.TemporaryDirectory(prefix='exec-mcp-standalone-') as directory:
        target = Path(directory) / 'target'
        subprocess.run(['cargo', 'build', '--locked', '--offline', '--release',
                        '--manifest-path', str(source / 'Cargo.toml'), '--target-dir', str(target)],
                       env=environment, check=True)
        binary = target / 'release/chatgpt-exec-mcp'
        content = binary.read_bytes()
        for private_path in [source, cargo_home, Path.home()]:
            if os.fsencode(private_path) in content:
                raise ValueError('binary still contains a builder path')
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation avoids replacing an existing artifact.
        with destination.open('xb') as output, binary.open('rb') as built:
            shutil.copyfileobj(built, output)
        destination.chmod(0o755)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    try:
        build(args.output.absolute())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f'standalone build failed: {error}\n')


if __name__ == '__main__':
    main()

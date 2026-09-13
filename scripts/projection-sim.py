#!/usr/bin/env python3
"""Compare two byte-range projection models using synthetic or local logs.

This is an offline experiment, not the Rust projector. Standard library only.
"""

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import tempfile


PATTERN = re.compile(rb'error:|FAILURE:|Caused by:')


def merge(ranges):
    result = []
    for start, end in sorted(ranges):
        if start >= end:
            continue
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(end, result[-1][1]))
        else:
            result.append((start, end))
    return result


def marker(size):
    return f'\n... {size} bytes omitted ...\n'.encode()


class Reader:
    def __init__(self, file):
        self.file = file
        self.bytes_read = 0

    def read(self, start, end):
        self.file.seek(start)
        data = self.file.read(max(0, end - start))
        self.bytes_read += len(data)
        return data


def render(reader, snapshot, ranges):
    start, end = snapshot
    chunks = []
    for left, right in merge(ranges):
        assert start <= left < right <= end
        if left > start:
            chunks.append(marker(left - start))
        chunks.append(reader.read(left, right))
        start = right
    if start < end:
        chunks.append(marker(end - start))
    return b''.join(chunks).decode('utf-8', errors='replace')


def head_tail(reader, snapshot, budget):
    start, end = snapshot
    size = end - start
    keep = min(size, max(0, budget - len(marker(size)))) if size > budget else size
    while True:
        ranges = merge([(start, start + keep // 2), (end - (keep - keep // 2), end)])
        text = render(reader, snapshot, ranges)
        excess = len(text.encode()) - budget
        if excess <= 0:
            return ranges, text
        if keep == 0:
            # Tiny budgets still have raw ranges in the report, even if no
            # readable omission marker fits (as with recovery metadata in MCP).
            return [], ''
        keep = max(0, keep - excess)


def diagnostic_ranges(reader, snapshot, scan_limit):
    start, end = snapshot
    if end - start <= scan_limit:
        windows = [(start, end)]
    else:
        windows = [(start, start + scan_limit // 2),
                   (end - (scan_limit - scan_limit // 2), end)]
    candidates = []
    for left, right in windows:
        lines = reader.read(left, right).splitlines(keepends=True)
        offsets = [left]
        for line in lines:
            offsets.append(offsets[-1] + len(line))
        for index, line in enumerate(lines):
            if PATTERN.search(line):
                candidates.append((offsets[max(0, index - 2)],
                                   offsets[min(len(lines), index + 3)]))
    return merge(candidates)


def diagnostics(reader, snapshot, budget, scan_limit):
    start, end = snapshot
    if end - start <= budget:
        return head_tail(reader, snapshot, budget)
    # Bound both scan I/O and display size. Reserve one marker per possible gap;
    # start with 20% head / 40% tail / 40% diagnostic source bytes.
    baseline_ranges, baseline_text = head_tail(reader, snapshot, budget)
    # A diagnostic already visible in head/tail is no reason to redistribute
    # the budget (e.g. a compiler's final "error: aborting" summary).
    candidates = [candidate for candidate in diagnostic_ranges(reader, snapshot, scan_limit)
                  if not covered(baseline_ranges, candidate)]
    marker_budget = len(marker(end - start))
    available = max(0, budget - marker_budget)
    allowance = available * 2 // 5
    chosen = []
    for left, right in candidates:
        cost = right - left + marker_budget
        if cost <= allowance:
            chosen.append((left, right))
            allowance -= cost
    if not chosen:
        return baseline_ranges, baseline_text
    head = available // 5
    spent = sum(right - left + marker_budget for left, right in chosen)
    tail = available - head - spent  # Unused diagnostic allowance goes to tail.
    ranges = merge([(start, start + head), *chosen, (end - tail, end)])
    text = render(reader, snapshot, ranges)
    if len(text.encode()) > budget:
        return baseline_ranges, baseline_text
    return ranges, text


@dataclass
class Case:
    name: str
    path: Path
    snapshots: list
    targets: list | None  # (diagnostic header range, full context range)


def fixtures(directory):
    noise = b'progress: compiling routine source file 0123456789\n' * 1300
    compiler = (b'> Task :app:compileJava\nMain.java:42\n'
                b'error: cannot find symbol\n    missing.call();\n    ^\n')
    gradle = (b'> Task :app:compileKotlin FAILED\nFAILURE: Build failed\n'
              b'* What went wrong:\nCaused by: compiler failure\n    at build.Task.run(Task.java:42)\n')
    cases = []

    def add(name, prefix, block=b'', suffix=b'', key=b'error:', split=None):
        data = prefix + block + suffix
        path = directory / f'{name}.log'
        path.write_bytes(data)
        targets = []
        if block:
            key_start = len(prefix) + block.index(key)
            line_end = data.find(b'\n', key_start)
            if line_end < 0:
                line_end = len(data)
            targets = [((key_start, line_end), (len(prefix), len(prefix) + len(block)))]
        snapshots = [(0, len(data))] if split is None else [(0, split), (split, len(data))]
        cases.append(Case(name, path, snapshots, targets))

    add('success', noise)
    add('early', b'build-start\n', compiler, noise)
    add('middle', noise, compiler, noise)
    add('late', noise, compiler, b'build-finished\n')
    add('gradle', noise, gradle, noise, key=b'FAILURE:')
    add('long-diagnostic', noise, b'error: ' + b'x' * 6000 + b'\n', noise)
    add('long-context', noise, compiler + b'    stack frame\n' * 80, noise)
    add('false-positives', noise + b'guide: error: is an example, no build failed\n' * 80,
        compiler, noise)
    add('unrecognized', noise, b'Main.kt:42: unresolved reference: missing\n', noise,
        key=b'unresolved')
    add('scan-gap', noise * 6, compiler, noise * 6)
    # Split the keyword itself, without carrying parser state between polls.
    add('poll-split', noise, compiler, noise,
        split=len(noise) + compiler.index(b'error:') + 3)
    return cases


def covered(ranges, target):
    return any(left <= target[0] and right >= target[1] for left, right in merge(ranges))


def evaluate(case, budget, scan_limit, method, output_dir):
    ranges, previews, read_bytes = [], [], 0
    with case.path.open('rb') as file:
        reader = Reader(file)
        for snapshot in case.snapshots:
            if method == 'head-tail':
                selected, text = head_tail(reader, snapshot, budget)
            else:
                selected, text = diagnostics(reader, snapshot, budget, scan_limit)
            assert len(text.encode()) <= budget, (case.name, method)
            assert all(snapshot[0] <= left < right <= snapshot[1] for left, right in selected)
            ranges.extend(selected)
            previews.append(text)
        read_bytes = reader.bytes_read
    result = {
        'case': case.name, 'method': method, 'budget_per_snapshot': budget,
        'snapshots': case.snapshots, 'ranges': merge(ranges),
        'diagnostics_kept': None if case.targets is None else sum(
            covered(ranges, header) for header, _ in case.targets),
        'contexts_kept': None if case.targets is None else sum(
            covered(ranges, context) for _, context in case.targets),
        'targets': None if case.targets is None else len(case.targets),
        'display_bytes': sum(len(text.encode()) for text in previews),
        'read_bytes': read_bytes,
    }
    if output_dir:
        for index, text in enumerate(previews):
            (output_dir / f'{case.name}-{budget}-{method}-{index}.txt').write_text(text, encoding='utf-8')
    return result


def positive(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--budget', type=positive, nargs='+', default=[1024, 4096])
    parser.add_argument('--scan-limit', type=positive, default=256 * 1024)
    parser.add_argument('--log', type=Path, action='append', default=[], help='also compare a local raw log')
    parser.add_argument('--output-dir', type=Path, help='save synthetic logs, previews and report.json')
    args = parser.parse_args()
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='projection-sim-') as temporary:
        cases = fixtures(args.output_dir or Path(temporary))
        for index, path in enumerate(args.log):
            cases.append(Case(f'local-{index}', path, [(0, path.stat().st_size)], None))
        results = []
        print('case               budget method       diag context display-B read-B')
        for case in cases:
            for budget in args.budget:
                for method in ('head-tail', 'diagnostics'):
                    result = evaluate(case, budget, args.scan_limit, method, args.output_dir)
                    results.append(result)
                    count = result['targets']
                    diag = '-' if count is None else f"{result['diagnostics_kept']}/{count}"
                    context = '-' if count is None else f"{result['contexts_kept']}/{count}"
                    print(f"{case.name:18} {budget:6} {method:12} {diag:>4} {context:>7} "
                          f"{result['display_bytes']:9} {result['read_bytes']:6}")
        if args.output_dir:
            (args.output_dir / 'report.json').write_text(json.dumps({
                'scan_limit': args.scan_limit, 'results': results,
            }, indent=2) + '\n')


if __name__ == '__main__':
    main()

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub start: u64,
    pub end: u64,
    pub stored_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct Projection {
    pub snapshot: Snapshot,
    pub output: String,
    pub truncated: bool,
    pub encoding_loss: bool,
}

pub fn project(
    mut snapshot: Snapshot,
    budget: usize,
    hold_incomplete_utf8: bool,
) -> io::Result<Projection> {
    let mut file = File::open(&snapshot.path)?;
    let take = budget.max(1) as u64;
    let mut bytes = Vec::new();
    let range = snapshot.end.saturating_sub(snapshot.start);
    let mut truncated = range > take;
    if !truncated {
        file.seek(SeekFrom::Start(snapshot.start))?;
        file.take(range).read_to_end(&mut bytes)?;
        if hold_incomplete_utf8
            && let Err(error) = std::str::from_utf8(&bytes)
            && error.error_len().is_none()
        {
            bytes.truncate(error.valid_up_to());
            snapshot.end = snapshot.start.saturating_add(bytes.len() as u64);
        }
    } else {
        let marker = omitted_marker(range.saturating_sub(take));
        if marker.len() as u64 >= take {
            let short = b"...";
            bytes.extend_from_slice(&short[..take.min(short.len() as u64) as usize]);
        } else {
            let content_budget = take - marker.len() as u64;
            let head_budget = content_budget / 2;
            let tail_budget = content_budget.saturating_sub(head_budget);
            let head = read_utf8_safe_head(&mut file, snapshot.start, range, head_budget as usize)?;
            let mut tail = read_utf8_safe_tail(
                &mut file,
                snapshot.start,
                snapshot.end,
                tail_budget as usize,
            )?;
            if hold_incomplete_utf8
                && let Err(error) = std::str::from_utf8(&tail)
                && error.error_len().is_none()
            {
                let dropped = tail.len().saturating_sub(error.valid_up_to());
                tail.truncate(error.valid_up_to());
                snapshot.end = snapshot.end.saturating_sub(dropped as u64);
            }
            let omitted = range
                .saturating_sub(head.len() as u64)
                .saturating_sub(tail.len() as u64);
            bytes.extend_from_slice(&head);
            bytes.extend_from_slice(omitted_marker(omitted).as_bytes());
            bytes.extend_from_slice(&tail);
        }
    }
    let (mut output, encoding_loss) = match String::from_utf8(bytes) {
        Ok(output) => (output, false),
        Err(error) => (String::from_utf8_lossy(error.as_bytes()).into_owned(), true),
    };
    if output.len() > budget.max(1) {
        truncate_string_bytes(&mut output, budget.max(1));
        truncated = true;
    }
    Ok(Projection {
        snapshot,
        output,
        truncated,
        encoding_loss,
    })
}

pub(crate) fn omitted_marker(bytes: u64) -> String {
    format!("\n... {bytes} bytes omitted ...\n")
}

pub(crate) fn read_utf8_safe_head(
    file: &mut File,
    start: u64,
    range: u64,
    budget: usize,
) -> io::Result<Vec<u8>> {
    if budget == 0 || range == 0 {
        return Ok(Vec::new());
    }
    let read_len = range.min((budget.saturating_add(3)) as u64);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::with_capacity(read_len as usize);
    Read::by_ref(file).take(read_len).read_to_end(&mut bytes)?;
    if bytes.len() <= budget {
        return Ok(bytes);
    }
    let limit = budget.min(bytes.len());
    let boundary = match std::str::from_utf8(&bytes) {
        Ok(text) => floor_char_boundary(text, limit),
        Err(error) if error.valid_up_to() >= limit => {
            let valid = unsafe { std::str::from_utf8_unchecked(&bytes[..error.valid_up_to()]) };
            floor_char_boundary(valid, limit)
        }
        Err(_) => limit,
    };
    bytes.truncate(boundary);
    Ok(bytes)
}

pub(crate) fn read_utf8_safe_tail(
    file: &mut File,
    start: u64,
    end: u64,
    budget: usize,
) -> io::Result<Vec<u8>> {
    let range = end.saturating_sub(start);
    if budget == 0 || range == 0 {
        return Ok(Vec::new());
    }
    let read_len = range.min((budget.saturating_add(3)) as u64);
    let read_start = end.saturating_sub(read_len);
    file.seek(SeekFrom::Start(read_start))?;
    let mut bytes = Vec::with_capacity(read_len as usize);
    Read::by_ref(file).take(read_len).read_to_end(&mut bytes)?;
    if bytes.len() <= budget {
        return Ok(bytes);
    }
    let target = bytes.len().saturating_sub(budget);
    let search_end = target.saturating_add(3).min(bytes.len());
    for boundary in target..=search_end {
        if std::str::from_utf8(&bytes[boundary..]).is_ok() {
            return Ok(bytes.split_off(boundary));
        }
    }
    Ok(bytes.split_off(target))
}

pub(crate) fn truncate_string_bytes(value: &mut String, budget: usize) {
    let boundary = floor_char_boundary(value, budget);
    value.truncate(boundary);
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

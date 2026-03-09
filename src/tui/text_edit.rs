use super::*;

pub(super) fn handle_text_key(text: &mut String, cursor: &mut usize, key: KeyEvent) {
    clamp_cursor(text, cursor);
    let alt = key.modifiers.contains(KeyModifiers::ALT);
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

    match key.code {
        KeyCode::Left => {
            if alt {
                move_cursor_word_left(text, cursor);
            } else {
                *cursor = prev_char_boundary(text, *cursor);
            }
        }
        KeyCode::Up => move_cursor_line_up(text, cursor),
        KeyCode::Right => {
            if alt {
                move_cursor_word_right(text, cursor);
            } else {
                *cursor = next_char_boundary(text, *cursor);
            }
        }
        KeyCode::Down => move_cursor_line_down(text, cursor),
        KeyCode::Home => *cursor = 0,
        KeyCode::End => *cursor = text.len(),
        KeyCode::Backspace => {
            if alt {
                delete_word_left(text, cursor);
            } else {
                delete_char_left(text, cursor);
            }
        }
        KeyCode::Enter => {
            insert_text(text, cursor, "\n");
        }
        KeyCode::Char(c) if !alt && !ctrl => {
            let mut s = [0u8; 4];
            insert_text(text, cursor, c.encode_utf8(&mut s));
        }
        _ => {}
    }
}

pub(super) fn insert_text(text: &mut String, cursor: &mut usize, input: &str) {
    if input.is_empty() {
        return;
    }
    clamp_cursor(text, cursor);
    let normalized = if input.contains('\r') {
        std::borrow::Cow::Owned(input.replace("\r\n", "\n").replace('\r', "\n"))
    } else {
        std::borrow::Cow::Borrowed(input)
    };
    text.insert_str(*cursor, &normalized);
    *cursor += normalized.len();
}

pub(super) fn clamp_cursor(text: &str, cursor: &mut usize) {
    *cursor = (*cursor).min(text.len());
    while *cursor > 0 && !text.is_char_boundary(*cursor) {
        *cursor -= 1;
    }
}

pub(super) fn prev_char_boundary(s: &str, idx: usize) -> usize {
    if idx == 0 {
        return 0;
    }
    s[..idx].char_indices().last().map(|(i, _)| i).unwrap_or(0)
}

pub(super) fn next_char_boundary(s: &str, idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    let mut iter = s[idx..].char_indices();
    let _ = iter.next();
    if let Some((offset, _)) = iter.next() {
        idx + offset
    } else {
        s.len()
    }
}

pub(super) fn char_before(s: &str, idx: usize) -> Option<char> {
    if idx == 0 {
        None
    } else {
        s[..idx].chars().next_back()
    }
}

pub(super) fn char_at(s: &str, idx: usize) -> Option<char> {
    if idx >= s.len() {
        None
    } else {
        s[idx..].chars().next()
    }
}

pub(super) fn move_cursor_word_left(text: &str, cursor: &mut usize) {
    let mut idx = *cursor;
    while idx > 0 {
        let Some(ch) = char_before(text, idx) else {
            break;
        };
        if ch.is_whitespace() {
            idx = prev_char_boundary(text, idx);
        } else {
            break;
        }
    }
    while idx > 0 {
        let Some(ch) = char_before(text, idx) else {
            break;
        };
        if !ch.is_whitespace() {
            idx = prev_char_boundary(text, idx);
        } else {
            break;
        }
    }
    *cursor = idx;
}

pub(super) fn move_cursor_word_right(text: &str, cursor: &mut usize) {
    let mut idx = *cursor;
    let len = text.len();
    while idx < len {
        let Some(ch) = char_at(text, idx) else {
            break;
        };
        if ch.is_whitespace() {
            idx = next_char_boundary(text, idx);
        } else {
            break;
        }
    }
    while idx < len {
        let Some(ch) = char_at(text, idx) else {
            break;
        };
        if !ch.is_whitespace() {
            idx = next_char_boundary(text, idx);
        } else {
            break;
        }
    }
    *cursor = idx;
}

pub(super) fn line_start(text: &str, cursor: usize) -> usize {
    text[..cursor].rfind('\n').map_or(0, |idx| idx + 1)
}

pub(super) fn line_end(text: &str, cursor: usize) -> usize {
    text[cursor..]
        .find('\n')
        .map_or(text.len(), |offset| cursor + offset)
}

pub(super) fn char_offset_in_line(text: &str, cursor: usize, start: usize) -> usize {
    text[start..cursor].chars().count()
}

pub(super) fn byte_index_for_char_offset(
    text: &str,
    start: usize,
    end: usize,
    char_offset: usize,
) -> usize {
    if start >= end {
        return start;
    }
    for (seen, (offset, _)) in text[start..end].char_indices().enumerate() {
        if seen == char_offset {
            return start + offset;
        }
    }
    end
}

pub(super) fn move_cursor_line_up(text: &str, cursor: &mut usize) {
    clamp_cursor(text, cursor);
    let curr_start = line_start(text, *cursor);
    if curr_start == 0 {
        return;
    }
    let target_col = char_offset_in_line(text, *cursor, curr_start);
    let prev_end = curr_start - 1;
    let prev_start = text[..prev_end].rfind('\n').map_or(0, |idx| idx + 1);
    *cursor = byte_index_for_char_offset(text, prev_start, prev_end, target_col);
}

pub(super) fn move_cursor_line_down(text: &str, cursor: &mut usize) {
    clamp_cursor(text, cursor);
    let curr_start = line_start(text, *cursor);
    let curr_end = line_end(text, *cursor);
    if curr_end == text.len() {
        return;
    }
    let target_col = char_offset_in_line(text, *cursor, curr_start);
    let next_start = curr_end + 1;
    let next_end = text[next_start..]
        .find('\n')
        .map_or(text.len(), |offset| next_start + offset);
    *cursor = byte_index_for_char_offset(text, next_start, next_end, target_col);
}

pub(super) fn delete_char_left(text: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let start = prev_char_boundary(text, *cursor);
    text.replace_range(start..*cursor, "");
    *cursor = start;
}

pub(super) fn delete_word_left(text: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let mut start = *cursor;
    while start > 0 {
        let Some(ch) = char_before(text, start) else {
            break;
        };
        if ch.is_whitespace() {
            start = prev_char_boundary(text, start);
        } else {
            break;
        }
    }
    while start > 0 {
        let Some(ch) = char_before(text, start) else {
            break;
        };
        if !ch.is_whitespace() {
            start = prev_char_boundary(text, start);
        } else {
            break;
        }
    }
    text.replace_range(start..*cursor, "");
    *cursor = start;
}

// Thin wrappers for prompt screen (backward compat)

pub(super) fn handle_prompt_text_key(app: &mut App, key: KeyEvent) {
    handle_text_key(
        &mut app.prompt.prompt_text,
        &mut app.prompt.prompt_cursor,
        key,
    );
}

pub(super) fn insert_prompt_text(app: &mut App, text: &str) {
    insert_text(
        &mut app.prompt.prompt_text,
        &mut app.prompt.prompt_cursor,
        text,
    );
}

// ---------------------------------------------------------------------------
// Pipeline key handling
// ---------------------------------------------------------------------------

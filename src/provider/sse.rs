/// Minimal SSE (Server-Sent Events) parser for streaming API responses.
pub(crate) struct SseParser {
    buf: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SseEvent {
    pub event_type: Option<String>,
    pub data: String,
}

impl SseParser {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn next_event(&mut self) -> Option<SseEvent> {
        loop {
            let boundary = find_double_newline(&self.buf)?;
            let raw_block: Vec<u8> = self.buf.drain(..boundary).collect();
            skip_separator(&mut self.buf);

            let block = String::from_utf8_lossy(&raw_block);
            let mut event_type: Option<String> = None;
            let mut data_lines: Vec<String> = Vec::new();

            for line in block.lines() {
                if line.starts_with(':') {
                    continue;
                }
                if let Some(rest) = line.strip_prefix("event:") {
                    event_type = Some(rest.trim().to_string());
                } else if let Some(rest) = line.strip_prefix("data:") {
                    let d = rest.strip_prefix(' ').unwrap_or(rest);
                    if d == "[DONE]" {
                        return None;
                    }
                    data_lines.push(d.to_string());
                }
            }

            if data_lines.is_empty() {
                continue;
            }

            return Some(SseEvent {
                event_type,
                data: data_lines.join("\n"),
            });
        }
    }
}

fn find_double_newline(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\n' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        if i + 3 < buf.len()
            && buf[i] == b'\r'
            && buf[i + 1] == b'\n'
            && buf[i + 2] == b'\r'
            && buf[i + 3] == b'\n'
        {
            return Some(i);
        }
    }
    None
}

fn skip_separator(buf: &mut Vec<u8>) {
    let skip = buf
        .iter()
        .take_while(|&&byte| byte == b'\n' || byte == b'\r')
        .count();
    if skip > 0 {
        buf.drain(..skip);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_event() {
        let mut p = SseParser::new();
        p.feed(b"event: message\ndata: hello\n\n");
        let evt = p.next_event().unwrap();
        assert_eq!(evt.event_type.as_deref(), Some("message"));
        assert_eq!(evt.data, "hello");
        assert!(p.next_event().is_none());
    }

    #[test]
    fn parse_data_only() {
        let mut p = SseParser::new();
        p.feed(b"data: world\n\n");
        let evt = p.next_event().unwrap();
        assert!(evt.event_type.is_none());
        assert_eq!(evt.data, "world");
    }

    #[test]
    fn parse_multi_line_data() {
        let mut p = SseParser::new();
        p.feed(b"data: line1\ndata: line2\n\n");
        let evt = p.next_event().unwrap();
        assert_eq!(evt.data, "line1\nline2");
    }

    #[test]
    fn parse_split_across_feeds() {
        let mut p = SseParser::new();
        p.feed(b"data: hel");
        assert!(p.next_event().is_none());
        p.feed(b"lo\n\n");
        let evt = p.next_event().unwrap();
        assert_eq!(evt.data, "hello");
    }

    #[test]
    fn parse_done_returns_none() {
        let mut p = SseParser::new();
        p.feed(b"data: [DONE]\n\n");
        assert!(p.next_event().is_none());
    }

    #[test]
    fn parse_comments_are_skipped() {
        let mut p = SseParser::new();
        p.feed(b": comment\ndata: value\n\n");
        let evt = p.next_event().unwrap();
        assert_eq!(evt.data, "value");
    }

    #[test]
    fn parse_multiple_events() {
        let mut p = SseParser::new();
        p.feed(b"data: first\n\ndata: second\n\n");
        assert_eq!(p.next_event().unwrap().data, "first");
        assert_eq!(p.next_event().unwrap().data, "second");
        assert!(p.next_event().is_none());
    }
}

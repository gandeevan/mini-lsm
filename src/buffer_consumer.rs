use std::{cell::RefCell, cmp::min};

pub struct BufferConsumer<'a> {
    buf: &'a [u8],
    pos: RefCell<usize>,
    remaining: RefCell<usize>,
}

impl<'a> BufferConsumer<'a> {
    pub fn new(buf: &[u8]) -> BufferConsumer {
        BufferConsumer {
            buf,
            pos: RefCell::<usize>::new(0),
            remaining: RefCell::<usize>::new(buf.len()),
        }
    }

    pub fn consume(&self, n: usize) -> &[u8] {
        let bytes_to_consume = min(n, *self.remaining.borrow());
        let start = *self.pos.borrow();
        let data = &self.buf[start..start + bytes_to_consume];
        *self.pos.borrow_mut() += bytes_to_consume;
        *self.remaining.borrow_mut() -= bytes_to_consume;
        data
    }

    pub fn done(&self) -> bool {
        *self.remaining.borrow() == 0
    }

    pub fn remaining(&self) -> usize {
        *self.remaining.borrow()
    }
}

#[cfg(test)]
mod tests {
    use super::BufferConsumer;

    #[test]
    fn consume() {
        let bytes: [u8; 10] = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let mut bc = BufferConsumer::new(&bytes);
        assert_eq!(bc.remaining(), 10);

        let result = bc.consume(1);
        assert_eq!([1], result);
        assert_eq!(bc.remaining(), 9);

        let result = bc.consume(2);
        assert_eq!([2, 2], result);
        assert_eq!(bc.remaining(), 7);

        let result = bc.consume(3);
        assert_eq!([3, 3, 3], result);
        assert_eq!(bc.remaining(), 4);

        let result = bc.consume(4);
        assert_eq!([4, 4, 4, 4], result);
        assert_eq!(bc.remaining(), 0);

        assert!(bc.done());

        let result: &[u8] = bc.consume(5);
        assert!(result.is_empty());
    }
}

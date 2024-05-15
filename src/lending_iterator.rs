/// A trait representing a lending iterator.
pub trait LendingIterator {
    /// The type returned by the iterator, which can borrow from `Self`.
    type Item<'a>
    where
        Self: 'a;

    /// Advances the iterator and returns the next item, if any.
    ///
    /// # Returns
    ///
    /// - `Some(item)`: If there is a next item, returns the borrowed item.
    /// - `None`: If there are no more items in the iterator.
    fn next<'a>(&'a mut self) -> Option<Self::Item<'a>>;
}

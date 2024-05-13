pub trait LendingIterator {
    // This is a GAT, meaning the type returned can borrow from `Self`
    type Item<'a>
    where
        Self: 'a;

    // The core method, similar to the standard iterator trait
    fn next<'a>(&'a mut self) -> Option<Self::Item<'a>>;
}

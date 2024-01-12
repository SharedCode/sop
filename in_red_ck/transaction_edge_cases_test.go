package in_red_ck

/*
  - Two transactions updating same item.
  - Two transactions updating different items with collision on 1 item.
  - Two transactions updating different items with no collision but items' keys are sequential/contiguous between the two.
  - One transaction updates a colliding item in 1st and a 2nd trans, updates the colliding item as last.
  - Transaction rolls back, new transaction is fine.
  - Transactions roll back, new completes fine.
  - Reader transaction succeeds.
  - Reader transaction fails commit when an item read was modified by another transaction in-flight.
  - [add more test cases here...]
*/

package com.sharedcode.sop;

public enum BTreeAction {
    Add(1),
    AddIfNotExist(2),
    Update(3),
    Upsert(4),
    Remove(5),
    Find(6),
    FindWithID(7),
    GetItems(8),
    GetValues(9),
    GetKeys(10),
    First(11),
    Last(12),
    IsUnique(13),
    Count(14),
    GetStoreInfo(15),
    UpdateKey(16),
    UpdateCurrentKey(17),
    GetCurrentKey(18),
    Next(19),
    Previous(20),
    GetCurrentValue(21);

    public final int value;

    BTreeAction(int value) {
        this.value = value;
    }
}

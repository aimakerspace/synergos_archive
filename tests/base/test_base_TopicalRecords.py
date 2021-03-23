#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in


# Libs
from tinydb import where

# Custom
from conftest import check_field_equivalence, check_insertion_or_update

##################
# Configurations #
##################



#######################
# Records Class Tests #
#######################

def test_TopicalRecords_get_related_metadata(topicalRecord_env):
    """ Tests if a topical archive can retrieve entries with related metadata
        correctly.

    # C1: Check that custom 'created_at' field was applied
    # C2: Check that all record keys are the same
    # C3: Check that all record values are the same
    """
    (
        topical_records, 
        _, test_key, test_ids, 
        _, _, 
        records, related_entries
    ) = topicalRecord_env
    all_related_records = topical_records._get_related_metadata(
        r_id=test_ids, 
        key=test_key
    )
    for related_subject, related_records in all_related_records.items():
        # C1
        assert len(related_records) == 1
        # C2
        related_record = related_records[0]
        related_ids = related_entries[related_subject]
        assert related_record[test_key] == related_ids
        # C3 - C5
        check_field_equivalence(
            records=records, 
            subject=related_subject, 
            key=test_key, 
            new_record=related_record
        )


def test_TopicalRecords_expand_record(topicalRecord_env):
    """ Tests if a topical archive can expand records with metadata retrieved
        from its downstream hierarchy

    # C1: Checks if the key `relations` is added on to the specified record
    # C2: Checks if the topical relationships captured are correct
    # C3: Checks if related record IDs retrieved are the correct  
    """
    (
        topical_records, 
        _, test_key, test_ids, 
        _, _, 
        records, related_entries
    ) = topicalRecord_env
    simulated_record = {test_key: test_ids}
    expanded_record = topical_records._expand_record(
        record=simulated_record,
        key=test_key
    )
    # C1
    assert 'relations' in expanded_record.keys()
    # C2
    related_records = expanded_record['relations']
    assert set(related_records.keys()) == set(topical_records.relations)
    for related_subject, related_records in related_records.items():
        assert len(related_records) == 1
        related_record = related_records[0]
        # C3
        related_ids = related_entries[related_subject]
        assert related_record[test_key] == related_ids


def test_records_create(topicalRecord_env):
    """ Tests if creation of topical records is self-consistent

    # C1: Check that custom 'created_at' field was applied
    # C2: Check that all record keys are the same
    # C3: Check that all record values are the same
    # C4: Check that specified record for subject given key exists
    # C5: Check that if record is duplicated under the same key, total no. of
        records within the database remains the same
    # C6: Check that if record is duplicated under the same key, the original
        record is overwitten instead
    """
    (
        topical_records, 
        test_subject, test_key, test_ids, 
        test_details, _,
        records, _
    ) = topicalRecord_env
    test_record = {test_key: test_ids, **test_details}
    topical_records.create(new_record=test_record)
    # C1 - C3
    check_field_equivalence(records, test_subject, test_key, test_record)
    # C4 - C6
    check_insertion_or_update(records, test_subject, test_key, test_record)


def test_records_read_all(topicalRecord_env):
    """ Tests if bulk reading of topical records is self-consistent

    # C1: Check that only 1 record exists (inherited from create())
    # C2 - C4: Check that the retrieved record is the same as the one that 
        exists in the database
    # C5 - C7: Check that the test record is the same as the one that exists in
        the database
    """
    (
        topical_records, 
        test_subject, test_key, test_ids, 
        test_details, _,
        records, _
    ) = topicalRecord_env
    test_record = {test_key: test_ids, **test_details}
    # C1
    all_records = topical_records.read_all()
    assert len(all_records) == 1
    # C2 - C4
    check_field_equivalence(records, test_subject, test_key, all_records[0])
    # C5 - C7
    check_field_equivalence(records, test_subject, test_key, test_record)


def test_records_read(topicalRecord_env):
    """ Tests if single reading of topical record is self-consistent

    # C1: Check that the test record is the same as the one that exists in the
        database
    # C2: Check that the retrieved record is the same as the one that exists
        in the database
    """
    (
        topical_records, 
        test_subject, test_key, test_ids, 
        test_details, _,
        records, _
    ) = topicalRecord_env
    test_record = {test_key: test_ids, **test_details}
    # C1
    check_field_equivalence(records, test_subject, test_key, test_record)
    # C2
    retrieved_record = topical_records.read(
        r_id=test_ids,
        f_key=test_key
    )
    check_field_equivalence(records, test_subject, test_key, retrieved_record)


def test_records_update(topicalRecord_env):
    """ Tests if a topical record can be updated correctly.

    # C1: Check that the original record was updated (not a copy)
    # C2: Check that the updated record is the same as the one that exists
        in the database
    """
    (
        topical_records, 
        test_subject, test_key, test_ids, 
        _, test_updates,
        records, _
    ) = topicalRecord_env
    retrieved_record = topical_records.read(
        r_id=test_ids,
        f_key=test_key
    )
    updated_record = topical_records.update(
        r_id=test_ids,
        updates=test_updates
    )
    # C1
    assert updated_record.doc_id == retrieved_record.doc_id
    # C2
    check_field_equivalence(records, test_subject, test_key, updated_record)


def test_records_delete(topicalRecord_env):
    """ Tests if a topical record can be deleted.

    # C1: Check that the original record was updated (not a copy)
    # C2: Check that the deleted record is the same as the one that existed
        in the database
    """
    (
        topical_records, 
        _, test_key, test_ids, 
        _, _,
        records, _
    ) = topicalRecord_env
    targeted_record = topical_records.read(
        r_id=test_ids,
        f_key=test_key
    )
    deleted_record = topical_records.delete(
        r_id=test_ids,
        key=test_key
    )
    # C1
    assert targeted_record.doc_id == deleted_record.doc_id
    # C2
    retrieved_record = topical_records.read(
        r_id=test_ids,
        f_key=test_key
    )
    assert retrieved_record is None
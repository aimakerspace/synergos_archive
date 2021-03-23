#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in


# Libs
from tinydb import TinyDB

# Custom
from conftest import check_field_equivalence, check_insertion_or_update

##################
# Configurations #
##################


#######################
# Records Class Tests #
#######################

def test_records_load_database(record_env):
    """ Tests if an archive's database can be loaded

    # C1: Check if the loaded database is a TinyDB instance
    """
    records, _, _, _, _, _ = record_env
    assert isinstance(records.load_database(), TinyDB)


def test_records_create(record_env):
    """ Tests if creation of records is self-consistent

    # C1: Check that custom 'created_at' field was applied
    # C2: Check that all record keys are the same
    # C3: Check that all record values are the same
    # C4: Check that specified record for subject given key exists
    # C5: Check that if record is duplicated under the same key, total no. of
        records within the database remains the same
    # C6: Check that if record is duplicated under the same key, the original
        record is overwitten instead
    """
    records, test_subject, test_key, test_ids, test_details, _ = record_env
    test_record = {test_key: test_ids, **test_details}
    records.create(
        subject=test_subject,
        key=test_key,
        new_record=test_record
    )
    # C1 - C3
    check_field_equivalence(records, test_subject, test_key, test_record)
    # C4 - C6
    check_insertion_or_update(records, test_subject, test_key, test_record)


def test_records_read_all(record_env):
    """ Tests if bulk reading of records is self-consistent

    # C1: Check that only 1 record exists (inherited from create())
    # C2 - C4: Check that the retrieved record is the same as the one that 
        exists in the database
    # C5 - C7: Check that the test record is the same as the one that exists in
        the database
    """
    records, test_subject, test_key, test_ids, test_details, _ = record_env
    test_record = {test_key: test_ids, **test_details}
    # C1
    all_records = records.read_all(subject=test_subject)
    assert len(all_records) == 1
    # C2
    check_field_equivalence(records, test_subject, test_key, all_records[0])
    # C3
    check_field_equivalence(records, test_subject, test_key, test_record)


def test_records_read(record_env):
    """ Tests if single reading of records is self-consistent

    # C1: Check that the test record is the same as the one that exists in the
        database
    # C2: Check that the retrieved record is the same as the one that exists
        in the database
    """
    records, test_subject, test_key, test_ids, test_details, _ = record_env
    test_record = {test_key: test_ids, **test_details}
    # C1
    check_field_equivalence(records, test_subject, test_key, test_record)
    # C2
    retrieved_record = records.read(
        subject=test_subject, 
        key=test_key, 
        r_id=test_ids
    )
    check_field_equivalence(records, test_subject, test_key, retrieved_record)


def test_records_update(record_env):
    """ Tests if a record can be updated correctly.

    # C1: Check that the original record was updated (not a copy)
    # C2: Check that the updated record is the same as the one that exists
        in the database
    """
    records, test_subject, test_key, test_ids, _, test_updates = record_env
    retrieved_record = records.read(test_subject, test_key, test_ids)
    updated_record = records.update(
        subject=test_subject, 
        key=test_key, 
        r_id=test_ids,
        updates=test_updates
    )
    # C1
    assert updated_record.doc_id == retrieved_record.doc_id
    # C2
    check_field_equivalence(records, test_subject, test_key, updated_record)


def test_records_delete(record_env):
    """ Tests if a record can be deleted.

    # C1: Check that the original record was updated (not a copy)
    # C2: Check that the deleted record is the same as the one that existed
        in the database
    """
    records, test_subject, test_key, test_ids, _, _ = record_env
    targeted_record = records.read(test_subject, test_key, test_ids)
    deleted_record = records.delete(        
        subject=test_subject, 
        key=test_key, 
        r_id=test_ids
    )
    # C1
    assert targeted_record.doc_id == deleted_record.doc_id
    # C2
    retrieved_record = records.read(test_subject, test_key, test_ids)
    assert retrieved_record is None
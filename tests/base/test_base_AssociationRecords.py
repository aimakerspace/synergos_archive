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

def test_AssociationRecords_create(associationRecord_env):
    """ Tests if creation of associated records is self-consistent

    # C1: Check that custom 'created_at' field was applied
    # C2: Check that all record keys are the same
    # C3: Check that all record values are the same
    # C4: Check that specified record for subject given key exists
    # C5: Check that if record is duplicated under the same key, total no. of
        records within the database remains the same
    # C6: Check that if record is duplicated under the same key, the original
        record is overwitten instead
    # C7: Check that link hierarchy was enforced correctly across associated
        records
    """
    (
        test_key, test_link, test_ids, 
        associated_record_hierarchy,
        records
    ) = associationRecord_env

    links = []
    for level in associated_record_hierarchy:
        associated_records, associated_details, _ = level
        test_record = {test_key: test_ids, **associated_details}
        created_record = associated_records.create(new_record=test_record)
        # C1 - C3
        check_field_equivalence(
            records=records, 
            subject=associated_records.subject, 
            key=test_key, 
            new_record=test_record
        )
        # C4 - C6
        check_insertion_or_update(
            records=records, 
            subject=associated_records.subject, 
            key=test_key, 
            new_record=test_record
        )
        links.append(created_record[test_link])

    for l_idx in range(1, len(links)):
        # C7
        upper_hierarchy_link = links[l_idx - 1]
        lower_hierarchy_link = links[l_idx]
        assert set(upper_hierarchy_link).issubset(set(lower_hierarchy_link)) 


def test_AssociationRecords_read_all(associationRecord_env):
    """ Tests if bulk reading of topical records is self-consistent

    # C1: Check that only 1 record exists (inherited from create())
    # C2 - C4: Check that the retrieved record is the same as the one that 
        exists in the database
    # C5: Check that all keys in test record are found in retrieved record
    # C6: Check that all values found in retrieved record are correct
    # C7: Check that link hierarchy was enforced correctly across associated
        records
    """
    (
        test_key, test_link, test_ids, 
        associated_record_hierarchy,
        records
    ) = associationRecord_env
    
    links = []
    ranked_subjects = []
    associated_entries = []
    for level in associated_record_hierarchy:
        associated_records, associated_details, _ = level
        test_record = {test_key: test_ids, **associated_details}
        test_subject = associated_records.subject
        # C1
        all_records = associated_records.read_all()
        assert len(all_records) == 1
        # C2 - C4
        retrieved_record = all_records[0]
        check_field_equivalence(
            records=records, 
            subject=test_subject, 
            key=test_key, 
            new_record=retrieved_record
        )
        # C5 - C6
        for k,v in test_record.items():
            assert k in retrieved_record.keys()
            assert v == retrieved_record[k]
        
        ranked_subjects.append(test_subject)
        associated_entries.append(associated_records.associations)
        links.append(retrieved_record[test_link])

    for idx in range(len(associated_entries), 0, -1):
        # C7
        associations = associated_entries[idx-1]
        associated_subjects = ranked_subjects[:idx-1]
        assert set(associations) == set(associated_subjects)

    for l_idx in range(1, len(links)):
        # C8
        upper_hierarchy_link = links[l_idx - 1]
        lower_hierarchy_link = links[l_idx]
        assert set(upper_hierarchy_link).issubset(set(lower_hierarchy_link)) 


def test_AssociationRecords_read(associationRecord_env):
    """ Tests if single reading of topical record is self-consistent

    # C1: Check that the test record is the same as the one that exists in the
        database
    # C2: Check that the retrieved record is the same as the one that exists
        in the database
    """
    (
        test_key, test_link, test_ids, 
        associated_record_hierarchy,
        records
    ) = associationRecord_env
    
    links = []
    ranked_subjects = []
    associated_entries = []
    for level in associated_record_hierarchy:
        associated_records, associated_details, _ = level
        test_record = {test_key: test_ids, **associated_details}
        test_subject = associated_records.subject
        # C1 - C3
        retrieved_record = associated_records.read(r_id=test_ids)
        check_field_equivalence(
            records=records, 
            subject=test_subject, 
            key=test_key, 
            new_record=retrieved_record
        )
        # C4 - C5
        for k,v in test_record.items():
            assert k in retrieved_record.keys()
            assert v == retrieved_record[k]
        
        ranked_subjects.append(test_subject)
        associated_entries.append(associated_records.associations)
        links.append(retrieved_record[test_link])

    for idx in range(len(associated_entries), 0, -1):
        # C6
        associations = associated_entries[idx-1]
        associated_subjects = ranked_subjects[:idx-1]
        assert set(associations) == set(associated_subjects)

    for l_idx in range(1, len(links)):
        # C8
        upper_hierarchy_link = links[l_idx - 1]
        lower_hierarchy_link = links[l_idx]
        assert set(upper_hierarchy_link).issubset(set(lower_hierarchy_link)) 


def test_AssociationRecords_update(associationRecord_env):
    """ Tests if a topical record can be updated correctly.

    # C1: Check that the original record was updated (not a copy)
    # C2: Check that the updated record is the same as the one that exists
        in the database
    """
    (
        test_key, _, test_ids, 
        associated_record_hierarchy,
        records
    ) = associationRecord_env

    for level in associated_record_hierarchy:
        associated_records, _, associated_updates = level
        retrieved_record = associated_records.read(r_id=test_ids)
        updated_record = associated_records.update(
            r_id=test_ids,
            updates=associated_updates
        )
        test_subject = associated_records.subject
        # C1
        assert updated_record.doc_id == retrieved_record.doc_id
        # C2
        check_field_equivalence(records, test_subject, test_key, updated_record)


def test_AssociationRecords_delete(associationRecord_env):
    """ Tests if a topical record can be deleted.

    # C1: Check that the original record was updated (not a copy)
    # C2: Check that the deleted record is the same as the one that existed
        in the database
    """
    (
        test_key, test_link, test_ids, 
        associated_record_hierarchy,
        records
    ) = associationRecord_env
    
    top_level_records, _, _ = associated_record_hierarchy.pop(0)
    targeted_top_record = top_level_records.read(r_id=test_ids)
    deleted_top_record = top_level_records.delete(r_id=test_ids)
    # C1
    assert targeted_top_record.doc_id == deleted_top_record.doc_id
    # C2
    retrieved_record = top_level_records.read(r_id=test_ids)
    assert retrieved_record is None

    for level in associated_record_hierarchy:
        associated_records, _, _ = level
        # C3
        retrieved_record = associated_records.read(r_id=test_ids)
        assert retrieved_record is None

#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in


# Libs


# Custom
from conftest import (
    check_key_equivalence,
    check_relation_equivalence,
    check_link_equivalence,
    check_detail_equivalence
)


##################
# Configurations #
##################


#################################
# ValidationRecords Class Tests #
#################################

def test_ValidationRecords_create(validation_env):
    """ Tests if creation of validation records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that validation records was dynamically created
    # C2: Check that validation records was archived with correct composite keys
    # C3: Check that validation records captured the correct validation details
    """
    (
        validation_records, validation_details, _,
        (collab_id, project_id, expt_id, run_id, participant_id)
    ) = validation_env
    created_validation = validation_records.create(
        participant_id=participant_id,
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id, 
        run_id=run_id,
        details=validation_details
    )
    check_key_equivalence(
        record=created_validation,
        ids=[participant_id, collab_id, project_id, expt_id, run_id]
    )
    check_link_equivalence(record=created_validation)
    check_detail_equivalence(
        record=created_validation,
        details=validation_details
    )


def test_ValidationRecords_read_all(validation_env):
    """ Tests if bulk reading of run records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that only 1 record exists (inherited from create())
    # C2: Check that run records was archived with correct composite keys
    # C3: Check hierarchy-enforcing field "relations" exist
    # C4: Check that run records captured the correct run details
    """
    (
        validation_records, validation_details, _,
        (collab_id, project_id, expt_id, run_id, participant_id)
    ) = validation_env
    all_validations = validation_records.read_all()

    assert len(all_validations) == 1

    for retrieved_record in all_validations:
        check_key_equivalence(
            record=retrieved_record,
            ids=[participant_id, collab_id, project_id, expt_id, run_id]
        )
        check_link_equivalence(record=retrieved_record)
        check_relation_equivalence(
            record=retrieved_record,
            r_type="validation"
        )
        check_detail_equivalence(
            record=retrieved_record,
            details=validation_details
        )


def test_ValidationRecords_read(validation_env):
    """ Tests if single reading of run records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that specified record exists (inherited from create())
    # C2: Check that run records was archived with correct composite keys
    # C3: Check hierarchy-enforcing field "relations" exist
    # C4: Check that hierarchies are accessible within "relations"
    # C5: Check that run records captured the correct run details
    """
    (
        validation_records, validation_details, _,
        (collab_id, project_id, expt_id, run_id, participant_id)
    ) = validation_env
    retrieved_validation = validation_records.read(
        participant_id=participant_id,
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id, 
        run_id=run_id,
    )
    assert retrieved_validation is not None
    check_key_equivalence(
        record=retrieved_validation,
        ids=[participant_id, collab_id, project_id, expt_id, run_id]
    )
    check_link_equivalence(record=retrieved_validation)
    check_relation_equivalence(
        record=retrieved_validation,
        r_type="validation"
    )
    check_detail_equivalence(
        record=retrieved_validation,
        details=validation_details
    )


def test_ValidationRecords_update(validation_env):
    """ Tests if a run record can be updated without breaking hierarchial
        relations.

    # C1: Check that the original run record was updated (not a copy)
    # C2: Check that run record values have been updated
    # C3: Check hierarchy-enforcing field "relations" did not change
    """
    targeted_alignment = validation_records.read(
        project_id=project_id,
        participant_id=participant_id
    )
    updated_alignment = validation_records.update(
        project_id=project_id,
        participant_id=participant_id,
        updates=alignment_updates
    )
    assert targeted_alignment.doc_id == updated_alignment.doc_id
    for k,v in alignment_updates.items():
        assert updated_alignment[k] == v   


def test_ValidationRecords_delete(validation_env):
    """ Tests if a run record can be deleted.

    # C1: Check that the original run record was deleted (not a copy)
    # C2: Check that specified run record no longer exists
    """
    targeted_alignment = validation_records.read(
        project_id=project_id,
        participant_id=participant_id
    )
    deleted_alignment = validation_records.delete(
        project_id=project_id,
        participant_id=participant_id
    )
    assert targeted_alignment.doc_id == deleted_alignment.doc_id
    assert validation_records.read(
        project_id=project_id,
        participant_id=participant_id
    ) is None
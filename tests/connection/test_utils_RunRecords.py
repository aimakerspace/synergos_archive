#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in


# Libs


# Custom
from conftest import generate_federated_combination, generate_run_info

##################
# Configurations #
##################

collab_id, project_id, expt_id, run_id, _ = generate_federated_combination()

run_details = generate_run_info()
run_updates = generate_run_info()

##########################
# RunRecords Class Tests #
##########################

def test_RunRecords_create(run_records):
    """ Tests if creation of run records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that run records was dynamically created
    # C2: Check that run records was archived with correct composite keys
    # C3: Check that run records captured the correct run details
    """
    created_run = run_records.create(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id,
        details=run_details
    )
    # C1
    assert 'created_at' in created_run.keys()
    # C2
    created_run.pop('created_at')
    key = created_run.pop('key')
    assert set([collab_id, project_id, expt_id, run_id]) == set(key.values())
    # C3
    assert created_run == run_details


def test_RunRecords_read_all(run_records, payload_hierarchy):
    """ Tests if bulk reading of run records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that only 1 record exists (inherited from create())
    # C2: Check that run records was archived with correct composite keys
    # C3: Check hierarchy-enforcing field "relations" exist
    # C4: Check that run records captured the correct run details
    """
    # C1
    all_runs = run_records.read_all()
    assert len(all_runs) == 1
    # C2
    retrieved_run = all_runs[0]
    key = retrieved_run.pop('key')
    assert set([collab_id, project_id, expt_id, run_id]) == set(key.values())
    # C3
    assert 'relations' in retrieved_run.keys()
    # C4
    assert all(
        relation_key in retrieved_run['relations'].keys()
        for relation_key in payload_hierarchy['run']
    )
    # C5
    retrieved_run.pop('created_at')
    retrieved_run.pop('relations')
    assert retrieved_run == run_details


def test_RunRecords_read(run_records, payload_hierarchy):
    """ Tests if single reading of run records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that specified record exists (inherited from create())
    # C2: Check that run records was archived with correct composite keys
    # C3: Check hierarchy-enforcing field "relations" exist
    # C4: Check that hierarchies are accessible within "relations"
    # C5: Check that run records captured the correct run details
    """
    retrieved_run = run_records.read(
        collab_id=collab_id,
        project_id=project_id, 
        expt_id=expt_id, 
        run_id=run_id
    )
    # C1
    assert retrieved_run is not None
    # C2
    key = retrieved_run.pop('key')
    assert set([collab_id, project_id, expt_id, run_id]) == set(key.values())
    # C3
    assert 'relations' in retrieved_run.keys()
    # C4
    assert all(
        relation_key in retrieved_run['relations'].keys()
        for relation_key in payload_hierarchy['run']
    )
    # C5
    retrieved_run.pop('created_at')
    retrieved_run.pop('relations')
    assert retrieved_run == run_details


def test_RunRecords_update(run_records):
    """ Tests if a run record can be updated without breaking hierarchial
        relations.

    # C1: Check that the original run record was updated (not a copy)
    # C2: Check that run record values have been updated
    # C3: Check hierarchy-enforcing field "relations" did not change
    """
    targeted_run = run_records.read(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id
    )
    run_records.update(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id,
        updates=run_updates
    )
    updated_run = run_records.read(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id
    )
    # C1
    assert targeted_run.doc_id == updated_run.doc_id
    # C2
    for k,v in run_updates.items():
        assert updated_run[k] == v
    # C3
    assert targeted_run['relations'] == updated_run['relations']


def test_RunRecords_delete(run_records):
    """ Tests if a run record can be deleted.

    # C1: Check that the original run record was deleted (not a copy)
    # C2: Check that specified run record no longer exists
    """
    targeted_run = run_records.read(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id
    )
    deleted_run = run_records.delete(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id
    )
    # C1
    assert targeted_run.doc_id == deleted_run.doc_id
    # C2
    assert run_records.read(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id
    ) is None


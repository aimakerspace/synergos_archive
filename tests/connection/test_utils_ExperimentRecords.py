#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in


# Libs


# Custom
from conftest import (
    generate_federated_combination, 
    generate_run_info,
    generate_experiment_info
)

##################
# Configurations #
##################

collab_id, project_id, expt_id, run_id, _ = generate_federated_combination()

expt_details = generate_experiment_info()
expt_updates = generate_experiment_info()

run_details = generate_run_info()

#################################
# ExperimentRecords Class Tests #
#################################

def test_ExperimentRecords_create(expt_records):
    """ Tests if creation of experiment records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that experiment records was dynamically created
    # C2: Check that experiment records was archived with correct composite keys
    # C3: Check that experiment records captured the correct run details
    """
    created_expt = expt_records.create(
        project_id=project_id,
        expt_id=expt_id,
        details=expt_details
    )
    # C1
    assert 'created_at' in created_expt.keys()
    # C2
    created_expt.pop('created_at')
    key = created_expt.pop('key')
    assert set([project_id, expt_id]) == set(key.values())
    # C3
    assert created_expt == expt_details


def test_ExperimentRecords_read_all(expt_records, payload_hierarchy):
    """ Tests if bulk reading of experiment records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that only 1 record exists (inherited from create())
    # C2: Check that experiment records was archived with correct composite keys
    # C3: Check hierarchy-enforcing field "relations" exist
    # C4: Check that experiment records captured the correct experiment details
    """
    # C1
    all_expts = expt_records.read_all()
    assert len(all_expts) == 1
    # C2
    retrieved_expt = all_expts[0]
    key = retrieved_expt.pop('key')
    assert set([project_id, expt_id]) == set(key.values())
    # C3
    assert 'relations' in retrieved_expt.keys()
    # C4
    assert all(
        relation_key in retrieved_expt['relations'].keys()
        for relation_key in payload_hierarchy['experiment']
    )
    # C5
    retrieved_expt.pop('created_at')
    retrieved_expt.pop('relations')
    assert retrieved_expt == expt_details


def test_ExperimentRecords_read(expt_records, payload_hierarchy):
    """ Tests if single reading of experiment records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that specified record exists (inherited from create())
    # C2: Check that experiment records was archived with correct composite keys
    # C3: Check hierarchy-enforcing field "relations" exist
    # C4: Check that hierarchies are accessible within "relations"
    # C5: Check that experiment records captured the correct experiment details
    """
    retrieved_expt = expt_records.read(
        project_id=project_id, 
        expt_id=expt_id
    )
    # C1
    assert retrieved_expt is not None
    # C2
    key = retrieved_expt.pop('key')
    assert set([project_id, expt_id]) == set(key.values())
    # C3
    assert 'relations' in retrieved_expt.keys()
    # C4
    assert all(
        relation_key in retrieved_expt['relations'].keys()
        for relation_key in payload_hierarchy['experiment']
    )
    # C5    
    retrieved_expt.pop('created_at')
    retrieved_expt.pop('relations')
    assert retrieved_expt == expt_details


def test_ExperimentRecords_update(expt_records):
    """ Tests if a experiment record can be updated without breaking 
        hierarchial relations.

    # C1: Check that the original experiment record was updated (not a copy)
    # C2: Check that experiment record values have been updated
    # C3: Check hierarchy-enforcing field "relations" did not change
    """
    targeted_expt = expt_records.read(
        project_id=project_id,
        expt_id=expt_id
    )
    expt_records.update(
        project_id=project_id,
        expt_id=expt_id,
        updates=expt_updates
    )
    updated_expt = expt_records.read(
        project_id=project_id,
        expt_id=expt_id
    )
    # C1
    assert targeted_expt.doc_id == updated_expt.doc_id
    # C2
    for k,v in expt_updates.items():
        assert updated_expt[k] == v   
    # C3
    assert targeted_expt['relations'] == updated_expt['relations']


def test_ExperimentRecords_delete(expt_records, run_records):
    """ Tests if a experiment record can be deleted.

    # C1: Check that the original experiment record was deleted (not a copy)
    # C2: Check that related run record was deleted (not a copy)
    # C4: Check that specified experiment record no longer exists
    # C4: Check that related run record no longer exists
    """
    # Register a run under current experiment
    created_run = run_records.create(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id,
        details=run_details
    )
    # Now perform experiment deletion, checking for cascading deletion into run
    targeted_expt = expt_records.read(
        project_id=project_id,
        expt_id=expt_id
    )
    deleted_expt = expt_records.delete(
        project_id=project_id,
        expt_id=expt_id
    )
    # C1
    assert targeted_expt.doc_id == deleted_expt.doc_id
    # C2
    deleted_run = deleted_expt['relations']['Run'][0]
    assert created_run.doc_id == deleted_run.doc_id
    # C3
    assert expt_records.read(
        project_id=project_id, 
        expt_id=expt_id
    ) is None
    # C4
    assert run_records.read(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id
    ) is None

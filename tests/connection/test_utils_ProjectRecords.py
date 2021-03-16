#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import random

# Libs


# Custom
from conftest import (
    TEST_PATH,
    generate_federated_combination,
    generate_project_info,
    generate_experiment_info,
    generate_run_info
)
from synarchive.connection import ProjectRecords, ExperimentRecords, RunRecords

##################
# Configurations #
##################

project_records = ProjectRecords(db_path=TEST_PATH)
expt_records = ExperimentRecords(db_path=TEST_PATH)
run_records = RunRecords(db_path=TEST_PATH)

project_details = generate_project_info()
project_updates = generate_project_info()

expt_details = generate_experiment_info()

run_details = generate_run_info()

collab_id, project_id, expt_id, run_id, _ = generate_federated_combination()

##############################
# ProjectRecords Class Tests #
##############################

def test_ProjectRecords_create():
    """ Test if creation of project records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that project records was dynamically created
    # C2: Check that project records was archived with correct composite keys
    # C3: Check that project records captured the correct project details
    """
    created_project = project_records.create(
        project_id=project_id,
        details=project_details
    )
    # C1
    assert 'created_at' in created_project.keys()
    # C2
    created_project.pop('created_at')
    key = created_project.pop('key')
    assert set([project_id]) == set(key.values())
    # C3
    assert created_project == project_details


def test_ProjectRecords_read_all():
    """ Test if bulk reading of project records is self-consistent and 
        hierarchy-enforcing.

    # C1: Check that only 1 record exists 
    # C2: Check that project records was archived with correct composite keys
    # C3: Check that project records captured the correct project details
    """
    all_projects = project_records.read_all()
    assert len(all_projects) == 1
    retrieved_project = all_projects[0]
    key = retrieved_project.pop('key')
    assert set([project_id]) == set(key.values())
    assert 'relations' in retrieved_project.keys()
    retrieved_project.pop('created_at')
    retrieved_project.pop('relations')
    assert retrieved_project == project_details


def test_ProjectRecords_read():
    """
    """
    retrieved_project = project_records.read(
        project_id=project_id
    )
    assert retrieved_project is not None
    key = retrieved_project.pop('key')
    assert set([project_id]) == set(key.values())
    assert 'relations' in retrieved_project.keys()
    retrieved_project.pop('created_at')
    retrieved_project.pop('relations')
    assert retrieved_project == project_details


def test_ProjectRecords_update():
    """
    """
    targeted_project = project_records.read(
        project_id=project_id
    )
    updated_project = project_records.update(
        project_id=project_id,
        updates=project_updates
    )
    assert targeted_project.doc_id == updated_project.doc_id
    for k,v in project_updates.items():
        assert updated_project[k] == v   


def test_ProjectRecords_delete():
    """
    """
    # Register an experiment under the current project
    created_expt = expt_records.create(
        project_id=project_id,
        expt_id=expt_id,  
        details=expt_details 
    )
    # Register a run under experiment
    created_run = run_records.create(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id,
        details=run_details
    )
    # Now perform project deletion, checking for cascading deletion into Experiment & Run
    targeted_project = project_records.read(
        project_id=project_id
    )
    deleted_project = project_records.delete(
        project_id=project_id
    )
    assert targeted_project.doc_id == deleted_project.doc_id
    assert project_records.read(project_id=project_id) is None
    assert created_expt.doc_id == deleted_project['relations']['Experiment'][0].doc_id
    assert created_run.doc_id == deleted_project['relations']['Run'][0].doc_id
    assert expt_records.read(
        project_id=project_id,
        expt_id=expt_id
    ) is None
    assert run_records.read(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id
    ) is None

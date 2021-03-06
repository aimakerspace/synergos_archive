#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import os
from typing import Dict

# Libs
import jsonschema
import tinydb

# Custom
from .base import TopicalRecords, AssociationRecords
from .config import SCHEMAS as schemas

##################
# Configurations #
##################


#############################################
# Data Storage Class - CollaborationRecords #
#############################################

class CollaborationRecords(TopicalRecords):

    def __init__(self, db_path: str):
        super().__init__(
            "Collaboration", 
            "collab_id", 
            db_path,
            *[
                "Project",
                "Experiment", 
                "Run", 
                "Registration", 
                "Tag", 
                "Alignment", 
                "Model",
                "Validation",
                "Prediction"
            ]
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(self, collab_id: str) -> Dict[str, str]:
        return {'collab_id': collab_id}

    ##################
    # Core Functions #
    ##################
    
    def create(self, collab_id: str, details: dict) -> dict:
        # Check that new details specified conforms to project schema
        jsonschema.validate(details, schemas["collaboration_schema"])
        collab_key = self.__generate_key(collab_id)
        new_collab = {'key': collab_key}
        new_collab.update(details)
        return super().create(new_collab)


    def read(self, collab_id: str) -> dict:
        collab_key = self.__generate_key(collab_id)
        return super().read(collab_key)


    def update(self, collab_id: str, updates: dict) -> dict:
        collab_key = self.__generate_key(collab_id)
        return super().update(collab_key, updates)


    def delete(self, collab_id: str) -> dict:
        collab_key = self.__generate_key(collab_id)
        return super().delete(collab_key)



#######################################
# Data Storage Class - ProjectRecords #
#######################################

class ProjectRecords(TopicalRecords):

    def __init__(self, db_path: str):
        super().__init__(
            "Project", 
            "project_id", 
            db_path,
            *[
                "Experiment", 
                "Run", 
                "Registration", 
                "Tag", 
                "Alignment", 
                "Model",
                "Validation",
                "Prediction"
            ]
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(
        self,
        collab_id: str, 
        project_id: str
    ) -> Dict[str, str]:
        return {'collab_id': collab_id, 'project_id': project_id}

    ##################
    # Core Functions #
    ##################
    
    def create(
        self,
        collab_id: str, 
        project_id: str,
        details: dict
    ) -> dict:
        # Check that new details specified conforms to project schema
        jsonschema.validate(details, schemas["project_schema"])
        project_key = self.__generate_key(collab_id, project_id)
        new_project = {'key': project_key}
        new_project.update(details)
        return super().create(new_project)


    def read(
        self,
        collab_id: str, 
        project_id: str
    ) -> dict:
        project_key = self.__generate_key(collab_id, project_id)
        return super().read(project_key)


    def update(
        self,
        collab_id: str, 
        project_id: str,
        updates: dict
    ) -> dict:
        project_key = self.__generate_key(collab_id, project_id)
        return super().update(project_key, updates)


    def delete(
        self,
        collab_id: str, 
        project_id: str
    ) -> dict:
        project_key = self.__generate_key(collab_id, project_id)
        return super().delete(project_key)



###########################################
# Data Storage Class - ParticipantRecords #
###########################################

class ParticipantRecords(TopicalRecords):

    def __init__(self,  db_path: str):
        super().__init__(
            "Participant", 
            "participant_id", 
            db_path,
            *["Registration", "Tag", "Alignment", "Validation", "Prediction"]
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(self, participant_id):
        return {"participant_id": participant_id}

    ##################
    # Core Functions #
    ##################

    def create(self, participant_id, details):
        # Check that new details specified conforms to project schema
        jsonschema.validate(details, schemas["participant_schema"])
        assert participant_id == details["id"] 
        participant_key = self.__generate_key(participant_id)
        new_participant = {'key': participant_key}
        new_participant.update(details)
        return super().create(new_participant)


    def read(self, participant_id):
        participant_key = self.__generate_key(participant_id)
        return super().read(participant_key)


    def update(self, participant_id, updates):
        participant_key = self.__generate_key(participant_id)
        return super().update(participant_key, updates)


    def delete(self, participant_id):
        participant_key = self.__generate_key(participant_id)
        return super().delete(participant_key)




##########################################
# Data Storage Class - ExperimentRecords #
##########################################

class ExperimentRecords(TopicalRecords):

    def __init__(self, db_path: str):
        super().__init__(
            "Experiment", 
            "expt_id", 
            db_path,
            *["Run", "Model", "Validation", "Prediction"]
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str
    ) -> Dict[str, str]:
        return {
            'collab_id': collab_id, 
            "project_id": project_id, 
            "expt_id": expt_id
        }

    ##################
    # Core Functions #
    ##################

    def create(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str,
        details: dict
    ) -> dict:
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["experiment_schema"])
        expt_key = self.__generate_key(collab_id, project_id, expt_id)
        new_expt = {'key': expt_key}
        new_expt.update(details)
        return super().create(new_expt)


    def read(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str
    ) -> dict:
        expt_key = self.__generate_key(collab_id, project_id, expt_id)
        return super().read(expt_key)


    def update(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str,
        updates: dict
    ) -> dict:
        expt_key = self.__generate_key(collab_id, project_id, expt_id)
        return super().update(expt_key, updates)


    def delete(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str
    ) -> dict:
        expt_key = self.__generate_key(collab_id, project_id, expt_id)
        return super().delete(expt_key)



###################################
# Data Storage Class - RunRecords #
###################################

class RunRecords(TopicalRecords):

    def __init__(self, db_path: str):
        super().__init__(
            "Run",  
            "run_id", 
            db_path,
            *["Model", "Validation", "Prediction"]
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str
    ) -> Dict[str, str]:
        return {
            'collab_id': collab_id,
            'project_id': project_id, 
            'expt_id': expt_id, 
            'run_id': run_id
        }

    ##################
    # Core Functions #
    ##################

    def create(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
        details: dict
    ) -> dict:
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["run_schema"])
        run_key = self.__generate_key(collab_id, project_id, expt_id, run_id)
        new_run = {'key': run_key}
        new_run.update(details)
        return super().create(new_run)


    def read(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
    ) -> dict:
        run_key = self.__generate_key(collab_id, project_id, expt_id, run_id)
        return super().read(run_key)


    def update(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
        updates: dict
    ) -> dict:
        run_key = self.__generate_key(collab_id, project_id, expt_id, run_id)
        return super().update(run_key, updates)


    def delete(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
    ) -> dict:
        run_key = self.__generate_key(collab_id, project_id, expt_id, run_id)
        return super().delete(run_key)



#################################################
# Data Storage Association class - Registration #
#################################################

class RegistrationRecords(AssociationRecords):
    """ RegistrationRecords documents associative records as a means to allow
        participants to interact with different projects and vice-versa. 
        Note: Associative records DO NOT have user-allocated IDs! They are
              auto-generated to be used as foreign keys in other downstream
              associative records. Registrations are associative records and
              will not have a registration ID as part of its composite key.
              Instead it will exist under the 'link' key.
    """
    def __init__(self,  db_path: str):
        super().__init__(
            subject="Registration",  
            identifier="registration_id", 
            db_path=db_path,
            # relations=["Project", "Participant", "Tag", "Alignment"]
            relations=["Tag", "Alignment"]
        )   # no upstream relations
        # Note: Registration has 2 hidden upstream relations

    ###########
    # Helpers #
    ###########

    def __generate_key(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str
    ) -> Dict[str, str]:
        return {
            'collab_id': collab_id,
            'project_id': project_id,
            'participant_id': participant_id, 
        }


    def __cross_link_subjects(
        self, 
        collab_id: str,
        project_id: str, 
        participant_id: str, 
        concise: bool = True
    ):
        relevant_records = {}
        # Retrieve relevant collaboration using specified collboration ID
        collaboration_records = CollaborationRecords(db_path=self.db_path)
        relevant_collaboration = collaboration_records.read(
            collab_id=collab_id
        )
        # Retrieve relevant project using specified project ID
        project_records = ProjectRecords(db_path=self.db_path)
        relevant_project = project_records.read(
            collab_id=collab_id,
            project_id=project_id
        )
        # Retrieve relevant participants using specified participant ID
        participant_records = ParticipantRecords(db_path=self.db_path)
        relevant_participant = participant_records.read(
            participant_id=participant_id
        )
        # Remove details from internals nesting relations
        if concise:
            relevant_collaboration.pop('relations')
            relevant_project.pop('relations')
            relevant_participant.pop('relations')

        relevant_records['collaboration'] = relevant_collaboration
        relevant_records['project'] = relevant_project
        relevant_records['participant'] = relevant_participant
        return relevant_records

    ##################
    # Core Functions #
    ##################

    def create(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str,
        details: dict
    ) -> dict:
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["registration_schema"])
        registration_key = self.__generate_key(collab_id, project_id, participant_id)
        new_registration = {'key': registration_key}
        new_registration.update(details)
        return super().create(new_registration)


    def read_all(self, filter: dict = {}) -> dict:
        all_registrations = super().read_all(filter=filter)
        cross_linked_registrations = []
        for registration in all_registrations:
            registration_key = registration['key']
            relevant_records = self.__cross_link_subjects(**registration_key)
            registration.update(relevant_records)
            cross_linked_registrations.append(registration)
        return cross_linked_registrations


    def read(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str
    ) -> dict:
        registration_key = self.__generate_key(collab_id, project_id, participant_id)
        registration = super().read(registration_key)
        if registration:
            relevant_records = self.__cross_link_subjects(**registration_key)
            registration.update(relevant_records)
        return registration


    def update(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str,
        updates: dict
    ) -> dict:
        registration_key = self.__generate_key(collab_id, project_id, participant_id)
        return super().update(registration_key, updates)


    def delete(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str
    ) -> dict:
        registration_key = self.__generate_key(collab_id, project_id, participant_id)
        return super().delete(registration_key)



###############################################
# Data Storage Association class - TagRecords #
###############################################

class TagRecords(AssociationRecords):
    """ TagRecords documents the child associations of a participant with its
        registered project, archiving data tags used to locate datasets to be
        loaded during FL training.   
        Note: Associative records DO NOT have user-allocated IDs! They are
              auto-generated to be used as foreign keys in other downstream
              associative records. Tags are associative records and will not 
              have a tag ID as part of its composite key.
    """
    def __init__(self,  db_path: str):
        super().__init__(
            "Tag",  
            "tag_id", 
            db_path,
            ["Alignment"],      # downstream relations
            *["Registration"]   # upstream relations
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str
    ) -> Dict[str, str]:
        return {
            'collab_id': collab_id,
            'project_id': project_id,
            'participant_id': participant_id, 
        }

    ##################
    # Core Functions #
    ##################

    def create(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str,
        details: dict
    ) -> dict:
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["tag_schema"])
        tag_key = self.__generate_key(collab_id, project_id, participant_id)
        new_tag = {'key': tag_key}
        new_tag.update(details)
        return super().create(new_tag)


    def read(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str
    ) -> dict:
        tag_key = self.__generate_key(collab_id, project_id, participant_id)
        return super().read(tag_key)


    def update(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str,
        updates: dict
    ) -> dict:
        tag_key = self.__generate_key(collab_id, project_id, participant_id)
        return super().update(tag_key, updates)


    def delete(
        self, 
        collab_id: str, 
        project_id: str, 
        participant_id: str
    ) -> dict:
        tag_key = self.__generate_key(collab_id, project_id, participant_id)
        return super().delete(tag_key)

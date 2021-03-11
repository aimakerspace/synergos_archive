#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import os
from typing import Dict

# Libs
import jsonschema

# Custom
from .base import TopicalRecords, AssociationRecords
from .config import SCHEMAS as schemas

##################
# Configurations #
##################


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

    def __generate_key(self, project_id: str) -> Dict[str, str]:
        return {"project_id": project_id}

    ##################
    # Core Functions #
    ##################
    
    def create(self, project_id, details):
        # Check that new details specified conforms to project schema
        jsonschema.validate(details, schemas["project_schema"])
        project_key = self.__generate_key(project_id)
        new_project = {'key': project_key}
        new_project.update(details)
        return super().create(new_project)


    def read(self, project_id):
        project_key = self.__generate_key(project_id)
        return super().read(project_key)


    def update(self, project_id, updates):
        project_key = self.__generate_key(project_id)
        return super().update(project_key, updates)


    def delete(self, project_id):
        project_key = self.__generate_key(project_id)
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
            *["Registration", "Tag", "Alignment"]
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

    def __generate_key(self, project_id, expt_id):
        return {"project_id": project_id, "expt_id": expt_id}

    ##################
    # Core Functions #
    ##################

    def create(self, project_id, expt_id, details):
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["experiment_schema"])
        expt_key = self.__generate_key(project_id, expt_id)
        new_expt = {'key': expt_key}
        new_expt.update(details)
        return super().create(new_expt)


    def read(self, project_id, expt_id):
        expt_key = self.__generate_key(project_id, expt_id)
        return super().read(expt_key)


    def update(self, project_id, expt_id, updates):
        expt_key = self.__generate_key(project_id, expt_id)
        return super().update(expt_key, updates)


    def delete(self, project_id, expt_id):
        expt_key = self.__generate_key(project_id, expt_id)
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

    def __generate_key(self, project_id, expt_id, run_id):
        return {"project_id": project_id, "expt_id": expt_id, "run_id": run_id}

    ##################
    # Core Functions #
    ##################

    def create(self, project_id, expt_id, run_id, details):
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["run_schema"])
        run_key = self.__generate_key(project_id, expt_id, run_id)
        new_run = {'key': run_key}
        new_run.update(details)
        return super().create(new_run)


    def read(self, project_id, expt_id, run_id):
        run_key = self.__generate_key(project_id, expt_id, run_id)
        return super().read(run_key)


    def update(self, project_id, expt_id, run_id, updates):
        run_key = self.__generate_key(project_id, expt_id, run_id)
        return super().update(run_key, updates)


    def delete(self, project_id, expt_id, run_id):
        run_key = self.__generate_key(project_id, expt_id, run_id)
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
            relations=["Project", "Participant", "Tag", "Alignment"]
        )
        # Note: Registration has 2 hidden upstream relations

    ###########
    # Helpers #
    ###########

    def __generate_key(self, project_id, participant_id):
        return {"project_id": project_id, "participant_id": participant_id}


    def __cross_link_subjects(self, project_id, participant_id, concise=True):
        relevant_records = {}
        # Retrieve relevant project using specified project ID
        project_records = ProjectRecords(db_path=self.db_path)
        relevant_project = project_records.read(
            project_id=project_id
        )
        # Retrieve relevant participants using specified participant ID
        participant_records = ParticipantRecords(db_path=self.db_path)
        relevant_participant = participant_records.read(
            participant_id=participant_id
        )
        # Remove details from internals nesting relations
        if concise:
            relevant_project.pop('relations')
            relevant_participant.pop('relations')
        relevant_records['project'] = relevant_project
        relevant_records['participant'] = relevant_participant
        # Check if relevant records accumulated are valid
        assert set(["project", "participant"]) == set(relevant_records.keys())
        assert None not in relevant_records.values()
        return relevant_records

    ##################
    # Core Functions #
    ##################

    def create(self, project_id, participant_id, details):
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["registration_schema"])
        registration_key = self.__generate_key(project_id, participant_id)
        new_registration = {'key': registration_key}
        new_registration.update(details)
        return super().create(new_registration)


    def read_all(self, filter={}):
        all_registrations = super().read_all(filter=filter)
        cross_linked_registrations = []
        for registration in all_registrations:
            registration_key = registration['key']
            relevant_records = self.__cross_link_subjects(**registration_key)
            registration.update(relevant_records)
            cross_linked_registrations.append(registration)
        return cross_linked_registrations


    def read(self, project_id, participant_id):
        registration_key = self.__generate_key(project_id, participant_id)
        registration = super().read(registration_key)
        if registration:
            relevant_records = self.__cross_link_subjects(**registration_key)
            registration.update(relevant_records)
        return registration


    def update(self, project_id, participant_id, updates):
        registration_key = self.__generate_key(project_id, participant_id)
        return super().update(registration_key, updates)


    def delete(self, project_id, participant_id):
        registration_key = self.__generate_key(project_id, participant_id)
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
            ["Alignment"],
            *["Registration"]
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(
        self, 
        project_id: str, 
        participant_id: str
    ) -> Dict[str, str]:
        return {
            "project_id": project_id, 
            "participant_id": participant_id
        }

    ##################
    # Core Functions #
    ##################

    def create(self, project_id, participant_id, details):
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["tag_schema"])
        tag_key = self.__generate_key(project_id, participant_id)
        new_tag = {'key': tag_key}
        new_tag.update(details)
        return super().create(new_tag)


    def read(self, project_id, participant_id):
        tag_key = self.__generate_key(project_id, participant_id)
        return super().read(tag_key)


    def update(self, project_id, participant_id, updates):
        tag_key = self.__generate_key(project_id, participant_id)
        return super().update(tag_key, updates)


    def delete(self, project_id, participant_id):
        tag_key = self.__generate_key(project_id, participant_id)
        return super().delete(tag_key)

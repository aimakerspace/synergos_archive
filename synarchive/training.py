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
from .base import AssociationRecords
from .config import SCHEMAS as schemas

##################
# Configurations #
##################


#####################################################
# Data Storage Association class - AlignmentRecords #
#####################################################

class AlignmentRecords(AssociationRecords):

    def __init__(self, db_path: str):
        super().__init__(
            "Alignment",  
            "alignment_id", 
            db_path,
            [],                         # no downstream relations
            *["Registration", "Tag"]    # trace upstream
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(self, project_id, participant_id):
        return {
            "project_id": project_id, 
            "participant_id": participant_id
        }

    ##################
    # Core Functions #
    ##################

    def create(self, project_id, participant_id, details):
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["alignment_schema"])
        alignment_key = self.__generate_key(project_id, participant_id)
        new_alignment = {'key': alignment_key}
        new_alignment.update(details)
        return super().create(new_alignment)


    def read(self, project_id, participant_id):
        alignment_key = self.__generate_key(project_id, participant_id)
        return super().read(alignment_key)


    def update(self, project_id, participant_id, updates):
        alignment_key = self.__generate_key(project_id, participant_id)
        return super().update(alignment_key, updates)


    def delete(self, project_id, participant_id):
        alignment_key = self.__generate_key(project_id, participant_id)
        return super().delete(alignment_key)



#################################################
# Data Storage Association class - ModelRecords #
#################################################

class ModelRecords(AssociationRecords):

    def __init__(self, db_path: str):
        super().__init__(
            subject="Model",  
            identifier="model_id", 
            db_path=db_path,
            relations=["Validation", "Prediction"]  # downstream relations
        )   # no need to trace upstream 

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
            "project_id": project_id,
            "expt_id": expt_id,
            "run_id": run_id
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
        jsonschema.validate(details, schemas["model_schema"])
        model_key = self.__generate_key(collab_id, project_id, expt_id, run_id)
        new_model = {'key': model_key}
        new_model.update(details)
        return super().create(new_model)


    def read(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
    ) -> dict:
        model_key = self.__generate_key(collab_id, project_id, expt_id, run_id)
        return super().read(model_key)


    def update(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
        updates
    ) -> dict:
        model_key = self.__generate_key(collab_id, project_id, expt_id, run_id)
        return super().update(model_key, updates)


    def delete(
        self, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
    ) -> dict:
        model_key = self.__generate_key(collab_id, project_id, expt_id, run_id)
        return super().delete(model_key)
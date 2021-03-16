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


#########################################
# MLFlow Key storage class - MLFRecords #
#########################################

class MLFRecords(TopicalRecords):
    """ 
    This class solely exists as a persistent storage of `experiment_id/run_id`
    mappings to MLFlow generated experiement IDs & run IDs respectively. This is
    due to the fact that each unique experiment/run name can only be assigned a
    single MLFlow ID. Any attempt to re-initialise a new experiment/run will not
    override the existing registries, raising `mlflow.exceptions.MlflowException`
    """
    def __init__(self, db_path: str):
        super().__init__(
            subject="MLFlow",  
            identifier="name", 
            db_path=db_path
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(self, project, name):
        return {"project": project, "name": name}

    ##################
    # Core Functions #
    ##################

    def create(self, project, name, details):
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["mlflow_schema"])
        mlf_key = self.__generate_key(project, name)
        new_entry = {'key': mlf_key}
        new_entry.update(details)
        return super().create(new_entry)


    def read(self, project, name):
        mlf_key = self.__generate_key(project, name)
        return super().read(mlf_key)


    def update(self, project, name, updates):
        mlf_key = self.__generate_key(project, name)
        return super().update(mlf_key, updates)


    def delete(self, project, name):
        mlf_key = self.__generate_key(project, name)
        return super().delete(mlf_key)



######################################################
# Data Storage Association class - ValidationRecords #
######################################################

class ValidationRecords(AssociationRecords):
    """ This class catalogues exported changes of both the global & local models
        as federated training is in progress. Unlike PredictionRecords, this
        table does not record statistics, but rather tracked values to be fed
        MLFlow for visualisations, as well as incentive calculation.
    """

    def __init__(self, db_path: str):
        super().__init__(
            "Validation",  
            "val_id", 
            db_path,
            [],         # no downstream relations
            *["Model"]  # upstream relations
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(
        self, 
        participant_id: str, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str
    ) -> Dict[str, str]:
        return {
            "participant_id": participant_id,
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
        participant_id: str,
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
        details: dict
    ) -> dict:
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["prediction_schema"])
        validation_key = self.__generate_key(
            participant_id, 
            collab_id,
            project_id, 
            expt_id, 
            run_id
        )
        new_validation = {'key': validation_key}
        new_validation.update(details)
        return super().create(new_validation)


    def read(
        self, 
        participant_id: str,
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
    ) -> dict:
        validation_key = self.__generate_key(
            participant_id, 
            collab_id,
            project_id, 
            expt_id, 
            run_id
        )
        return super().read(validation_key)


    def update(
        self, 
        participant_id: str,
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
        updates: dict
    ) -> dict:
        validation_key = self.__generate_key(
            participant_id, 
            collab_id,
            project_id, 
            expt_id, 
            run_id
        )
        return super().update(validation_key, updates)


    def delete(
        self, 
        participant_id: str,
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
    ) -> dict:
        validation_key = self.__generate_key(
            participant_id, 
            collab_id,
            project_id, 
            expt_id, 
            run_id
        )
        return super().delete(validation_key)



######################################################
# Data Storage Association class - PredictionRecords #
######################################################

class PredictionRecords(AssociationRecords):

    def __init__(self, db_path: str):
        super().__init__(
            "Prediction",  
            "pred_id", 
            db_path,
            [],
            *["Model", "Registration", "Tag"]
        )

    ###########
    # Helpers #
    ###########

    def __generate_key(
        self, 
        participant_id: str, 
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str
    ) -> Dict[str, str]:
        return {
            "participant_id": participant_id,
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
        participant_id: str,
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
        details: dict
    ) -> dict:
        # Check that new details specified conforms to experiment schema
        jsonschema.validate(details, schemas["prediction_schema"])
        prediction_key = self.__generate_key(
            participant_id, 
            collab_id,
            project_id, 
            expt_id, 
            run_id
        )
        new_prediction = {'key': prediction_key}
        new_prediction.update(details)
        return super().create(new_prediction)


    def read(
        self, 
        participant_id: str,
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
    ) -> dict:
        prediction_key = self.__generate_key(
            participant_id, 
            collab_id,
            project_id, 
            expt_id, 
            run_id
        )
        return super().read(prediction_key)


    def update(
        self, 
        participant_id: str,
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
        updates: dict
    ) -> dict:
        prediction_key = self.__generate_key(
            participant_id, 
            collab_id,
            project_id, 
            expt_id, 
            run_id
        )
        return super().update(prediction_key, updates)


    def delete(
        self, 
        participant_id: str,
        collab_id: str, 
        project_id: str, 
        expt_id: str, 
        run_id: str,
    ) -> dict:
        prediction_key = self.__generate_key(
            participant_id, 
            collab_id,
            project_id, 
            expt_id, 
            run_id
        )
        return super().delete(prediction_key)
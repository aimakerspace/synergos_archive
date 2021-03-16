#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import copy
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from string import Template
from typing import Dict, List

# Libs
import pytest

# Custom
from synarchive.connection import (
    ProjectRecords, ExperimentRecords, RunRecords,
    ParticipantRecords, RegistrationRecords, TagRecords
)
from synarchive.training import AlignmentRecords, ModelRecords
from synarchive.evaluation import ValidationRecords, PredictionRecords

##################
# Configurations #
##################

TEST_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 
    "test_database.json"
)

ID_KEYS = {
    'collaboration': 'collab_id',
    'project': 'project_id',
    'experiment': 'expt_id',
    'run': 'run_id',
    'participant': 'participant_id',
    'registration': 'registration_id',
    'tag': 'tag_id',
    'alignment': 'alignment_id',
    'model': 'model_id',
    'validation': 'val_id',
    'prediction': 'pred_id'
}

RELATIONS_MAPPINGS = {
    # TTP-orchestrated mappings
    'collaboration': [
        "Project", "Experiment", "Run", "Model", "Validation", "Prediction",
        "Registration", "Tag", "Alignment" 
    ],
    'project': [
        "Experiment", "Run", "Model", "Validation", "Prediction",
        "Registration", "Tag", "Alignment" 
    ],
    'experiment': ["Run", "Model", "Validation", "Prediction"],
    'run': ["Model", "Validation", "Prediction"],
    'model': ["Validation", "Prediction"],
    'validation': [],
    'prediction': [],
    
    # Worker-orchestrated mappings
    'participant': ["Registration", "Tag", "Alignment"],
    'registration': ["Tag", "Alignment"],
    'tag': ["Alignment"],
    'alignment': []
}

LINK_MAPPINGS = {
    # TTP-orchestrated mappings
    'model': [],
    'validation': ["Model"],
    'prediction': ["Model"],
    
    # Worker-orchestrated mappings
    'registration': [],
    'tag': ["Registration"],
    'alignment': ["Registration", "Tag"]
}

COLLAB_PREFIX = Template("COLLAB_$collab_id")
PROJECT_PREFIX = Template("PROJECT_$project_id")
EXPT_PREFIX = Template("EXPT_$expt_id")
RUN_PREFIX = Template("RUN_$run_id")
PARTICIPANT_PREFIX = Template("PARTICIPANT_$participant_id")
PREFIXES = {
    'collaboration': COLLAB_PREFIX,
    'project': PROJECT_PREFIX,
    'experiment': EXPT_PREFIX,
    'run': RUN_PREFIX,
    'participant': PARTICIPANT_PREFIX
}

###########
# Helpers #
###########

def simulate_choice() -> bool:
    return random.random() < 0.5


def simulate_ip() -> str:
    return "{}.{}.{}.{}".format(
        random.randint(1, 1000),
        random.randint(1, 1000),
        random.randint(1, 1000),
        random.randint(1, 1000)
    )


def simulate_setup(
    collabs: int = 2,
    projects: int = 2,
    experiments: int = 2,
    runs: int = 3,
    particpants: int = 5
) -> Dict[str, List[Dict[str, str]]]:
    """ Automates generation of test keys for core topical records.

    Args:
        collabs (int): No. of collaborations to simulate
        projects (int): No. of projects per collab to simulate
        experiments (int): No. of experiments per project to simulate
        runs (int): No. of runs per experiment to simulate
        participants (int): No. of participants per collaboration to simulate
    Returns:
        All simulated keys (dict)
    """
    def construct_id(core_idx: int):
        return "-".join([str(core_idx), str(uuid.uuid4())])

    def update_topic_keys(key_dict, topic, new_key):
        topic_entries = key_dict.get(topic, [])
        topic_entries.append(new_key)
        key_dict.update({topic: topic_entries})

    def create_key(key_dict, topic, idx, hierarchy):
        topic_prefix = PREFIXES.get(topic)
        topic_id_key = ID_KEYS.get(topic)
        topic_id = topic_prefix.substitute({topic_id_key: construct_id(idx)})
        topic_key = {**hierarchy, topic_id_key: topic_id}
        update_topic_keys(key_dict, topic, topic_key)
        return topic_key

    # Generate unique collaborations
    all_keys = {}
    for collab_idx in range(collabs): 
        collab_key = create_key(
            key_dict=all_keys, 
            topic="collaboration", 
            idx=collab_idx, 
            hierarchy={}
        )

        # Generate unique projects
        for project_idx in range(projects):
            project_key = create_key(
                key_dict=all_keys, 
                topic="project", 
                idx=project_idx, 
                hierarchy=collab_key
            )

            # Generate unique experiments
            for expt_idx in range(experiments):
                expt_key = create_key(
                    key_dict=all_keys, 
                    topic="experiment", 
                    idx=expt_idx, 
                    hierarchy=project_key
                )

                # Generate unique runs
                for run_idx in range(runs):
                    run_key = create_key(
                        key_dict=all_keys, 
                        topic="run", 
                        idx=run_idx, 
                        hierarchy=expt_key
                    )

    # Generate unique participants
    for participant_idx in range(particpants):
        participant_key = create_key(
            key_dict=all_keys, 
            topic="participant", 
            idx=participant_idx, 
            hierarchy={}
        )

    return all_keys


def generate_federated_combination():
    """
    """
    test_keys = simulate_setup()
    
    federated_combination = random.choice(test_keys['run']) # lowest hierarchy
    collab_id = federated_combination[ID_KEYS['collaboration']]
    project_id = federated_combination[ID_KEYS['project']]
    expt_id = federated_combination[ID_KEYS['experiment']]
    run_id = federated_combination[ID_KEYS['run']]
    
    participant = random.choice(test_keys['participant'])
    participant_id = participant.get(ID_KEYS['participant'])
    return collab_id, project_id, expt_id, run_id, participant_id


def create_collaboration():
    """
    """
    return {}


def generate_project_info():
    """
    """
    return {
        "universe_alignment": [],
        "incentives": {},
        "start_at": datetime.strptime(
            datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            '%Y-%m-%d %H:%M:%S'
        ) + timedelta(hours=random.randint(0, 1000))
    }


def generate_experiment_info():
    """
    """
    return {
        "model": [
            {
                "is_input": True,
                "structure": {
                    "in_features": random.randint(1, 1000),
                    "out_features": random.randint(1, 100),
                    "bias": simulate_choice()
                },
                "l_type": "linear",
                "activation": "sigmoid"
            }
        ]
    }


def generate_run_info():
    """
    """
    return {
        "input_size": random.randint(1, 1000),
        "output_size": random.randint(1, 100),
        "batch_size": random.randint(1, 5000),
        "lr": random.random(),
        "weight_decay": random.random(),
        "rounds": random.randint(0, 100),
        "epochs": random.randint(0, 100),
        "lr_decay": random.random(),
        "seed": random.randint(1, 1000),
        "is_condensed": simulate_choice(),
        "precision_fractional": random.randint(1, 10),
        "use_CLR": simulate_choice(),
        "mu": random.random(),
        "reduction": random.choice(["none", "mean", "sum"]),
        "l1_lambda": random.random(),
        "l2_lambda": random.random(),
        "dampening": random.random(),
        "base_lr": random.random(),
        "max_lr": random.random(),
        "step_size_up": random.randint(1, 10),
        "step_size_down": random.randint(1, 10),
        "mode": random.choice(["triangular", "triangular2", "exp_range"]),
        "gamma": random.random(),
        "scale_mode": random.choice(["cycle", "iterations"]),
        "cycle_momentum": simulate_choice(),
        "base_momentum": random.random(),
        "max_momentum": random.random(),
        "last_epoch": random.randint(1, 10),
        "patience": random.randint(1, 10),
        "delta": random.random(),
        "cumulative_delta": simulate_choice()
    }


def generate_participant_info():
    """
    """
    return {
        "host": simulate_ip(),
        "f_port": random.randint(1001, 65535),
        "port": random.randint(1001, 65535),
        "log_msgs": simulate_choice(),
        "verbose": simulate_choice()
    }


def generate_registration_info():
    """
    """
    return {}


def generate_tag_info():
    """
    """
    def simulate_tag():
        dataset_name = "test_dataset"
        dataset_set = f"set_{str(random.randint(0, 10))}"
        version = f"version_{str(random.randint(0, 10))}"
        return [dataset_name, dataset_set, version]


    return {
        meta: [simulate_tag() for _ in range(random.randint(1, 10))] 
        for meta in ["train", "evaluate", "predict"]
    }


def generate_alignment_info():
    """
    """
    def simulate_alignment() -> List[int]:
        X_alignments = [
            random.randint(0, 1000) 
            for _ in range(random.randint(1, 100))
        ]
        y_alignments = [
            random.randint(0, 10) 
            for _ in range(random.randint(1, 10))
        ]
        return {'X': X_alignments, 'y': y_alignments}

    return {
        meta: simulate_alignment() 
        for meta in ["train", "evaluate", "predict"]
    }


def generate_model_info():
    """
    """
    return {
        "global": {
            "checkpoints": {
                "round_0": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_0/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_0/global_model.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_1/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_1/global_model.pt"
                    }
                },
                "round_1": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_0/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_0/global_model.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_1/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_1/global_model.pt"
                    }
                },
                "round_2": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_0/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_0/global_model.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_1/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_1/global_model.pt"
                    }
                },
                "round_3": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_0/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_0/global_model.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_1/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_1/global_model.pt"
                    }
                },
                "round_4": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_0/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_0/global_model.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_1/global_loss_history.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_1/global_model.pt"
                    }
                }
            },
            "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/global_loss_history.json",
            "origin": "ttp",
            "path": "/ttp/outputs/test_project_1/test_experiment/test_run/global_model.pt"
        },
        "local_1": {
            "checkpoints": {
                "round_0": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_0/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_0/local_model_test_participant_1.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_1/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_1/local_model_test_participant_1.pt"
                    }
                },
                "round_1": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_0/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_0/local_model_test_participant_1.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_1/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_1/local_model_test_participant_1.pt"
                    }
                },
                "round_2": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_0/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_0/local_model_test_participant_1.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_1/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_1/local_model_test_participant_1.pt"
                    }
                },
                "round_3": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_0/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_0/local_model_test_participant_1.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_1/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_1/local_model_test_participant_1.pt"
                    }
                },
                "round_4": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_0/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_0/local_model_test_participant_1.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_1/local_loss_history_test_participant_1.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_1/local_model_test_participant_1.pt"
                    }
                }
            },
            "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/local_loss_history_test_participant_1.json",
            "origin": "test_participant_1",
            "path": "/ttp/outputs/test_project_1/test_experiment/test_run/local_model_test_participant_1.pt"
        },
        "local_2": {
            "checkpoints": {
                "round_0": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_0/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_0/local_model_test_participant_2.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_1/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_0/epoch_1/local_model_test_participant_2.pt"
                    }
                },
                "round_1": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_0/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_0/local_model_test_participant_2.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_1/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_1/epoch_1/local_model_test_participant_2.pt"
                    }
                },
                "round_2": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_0/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_0/local_model_test_participant_2.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_1/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_2/epoch_1/local_model_test_participant_2.pt"
                    }
                },
                "round_3": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_0/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_0/local_model_test_participant_2.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_1/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_3/epoch_1/local_model_test_participant_2.pt"
                    }
                },
                "round_4": {
                    "epoch_0": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_0/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_0/local_model_test_participant_2.pt"
                    },
                    "epoch_1": {
                        "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_1/local_loss_history_test_participant_2.json",
                        "path": "/ttp/outputs/test_project_1/test_experiment/test_run/checkpoints/round_4/epoch_1/local_model_test_participant_2.pt"
                    }
                }
            },
            "loss_history": "/ttp/outputs/test_project_1/test_experiment/test_run/local_loss_history_test_participant_2.json",
            "origin": "test_participant_2",
            "path": "/ttp/outputs/test_project_1/test_experiment/test_run/local_model_test_participant_2.pt"
        }
    }


def generate_mlflow_info():
    """
    """
    expt_tracked = {
        "mlflow_id": "0",
        "mlflow_type": "experiment",
        "mlflow_uri": "/ttp/mlflow/test_project_1",
        "name": "test_experiment",
        "project": "test_project_1"
    }
    run_tracked = {
        "mlflow_id": "f81c4ff2c4704a0da07da1d16b4dfe9e",
        "mlflow_type": "run",
        "mlflow_uri": "/ttp/mlflow/test_project_1",
        "name": "test_run",
        "project": "test_project_1"
    }
    return expt_tracked, run_tracked


def generate_inference_info(
    action: str, 
    label_count: int = 1, 
    meta: str = "evaluate"
):
    """
    """
    def simulate_float_stats():
        return [random.random() for _ in range(label_count)]

    def simulate_int_stats():
        return [random.randint(0, 5000) for _ in range(label_count)]

    classification_stats = {
        "FDRs": simulate_float_stats(),
        "FNRs": simulate_float_stats(),
        "FNs": simulate_int_stats(),
        "FPRs": simulate_float_stats(),
        "FPs": simulate_int_stats(),
        "NPVs": simulate_float_stats(),
        "PPVs": simulate_float_stats(),
        "TNRs": simulate_float_stats(),
        "TNs": simulate_int_stats(),
        "TPRs": simulate_float_stats(),
        "TPs": simulate_int_stats(),
        "accuracy": simulate_float_stats(),
        "f_score": simulate_float_stats(),
        "pr_auc_score": simulate_float_stats(),
        "roc_auc_score": simulate_float_stats()
    }

    regression_stats = {
        'R2': random.random(),
        'MSE': random.random(), 
        'MAE': random.random()
    }

    return {
        meta: {
            "res_path": f"/worker/outputs/test_collaboration/test_project/test_experiment/test_run/{meta}/inference_statistics_{meta}.json",
            "statistics": (
                regression_stats 
                if action == "regress" 
                else classification_stats
            )
        }
    }


def reset_database(archive):
    """
    """
    database = archive.load_database()
    database.purge()


####################################
# Association Evaluation Functions #
####################################

# def check_equivalence_and_format(record, ):
#     assert 'created_at' in record.keys()
#     record.pop('created_at')
#     assert "key" in record.keys()
#     key = record.pop('key')
#     assert set([project_id, participant_id]) == set(key.values())
#     assert "link" in record.keys()
#     link = record.pop('link')
#     assert not set(link.items()).issubset(set(key.items()))
#     return record 

# def check_relation_equivalence_and_format(record):
#     """ Tests if creation of validation records is self-consistent and 
#         hierarchy-enforcing.

#     # C1: Check that validation records was dynamically created
#     # C2: Check that validation records was archived with correct composite keys
#     # C3: Check that validation records captured the correct validation details
#     """
#     assert 'relations' in record.keys()
#     relations = record.pop('relations')
#     assert (set(relations.keys()) == set())
#     return record 

# def check_detail_equivalence(details):
#     assert details == alignment_details
#     return details


def check_key_equivalence(record, ids):
    """
    """
    # Ensure that a cloned record is no different from its original
    cloned_record = copy.deepcopy(record)
    assert cloned_record == record
    # C1
    assert 'created_at' in cloned_record.keys()
    # C2
    assert "key" in cloned_record.keys()
    # C3
    key = cloned_record.pop('key')
    assert set(ids) == set(key.values())


def check_relation_equivalence(record, r_type):
    """
    """
    # Ensure that a cloned record is no different from its original
    cloned_record = copy.deepcopy(record)
    assert cloned_record == record
    # C1
    assert 'relations' in cloned_record.keys()
    # C2
    relations = cloned_record.pop('relations')
    assert (set(relations.keys()) == set(RELATIONS_MAPPINGS[r_type]))


def check_link_equivalence(record):
    """
    """
    # Ensure that a cloned record is no different from its original
    cloned_record = copy.deepcopy(record)
    assert cloned_record == record
    # C1
    assert "link" in record.keys()
    # C2
    key = cloned_record.pop('key')
    link = cloned_record.pop('link')
    assert not set(link.items()).issubset(set(key.items()))


def check_detail_equivalence(record, details):
    """
    """
    # Ensure that a cloned record is no different from its original
    cloned_record = copy.deepcopy(record)
    assert cloned_record == record
    # C1
    cloned_record.pop('created_at')
    cloned_record.pop('key')
    cloned_record.pop('link', None)       # Only AssociationRecords has this!
    cloned_record.pop('relations', None)  # Only .read & .read_all has this!
    assert details == cloned_record


##########################
# Miscellaneous Fixtures #
##########################

@pytest.fixture
def payload_hierarchy():
    return {
        'experiment': ['Run', 'Model', 'Validation', 'Prediction'],
        'run': ['Model', 'Validation', 'Prediction'],
        'validation': []
    }

######################
# Component Fixtures #
######################

@pytest.fixture
def collab_records():
    pass


@pytest.fixture
def project_records():
    pass


@pytest.fixture(scope='session')
def expt_records():
    expt_records = ExperimentRecords(db_path=TEST_PATH)
    reset_database(expt_records)
    return expt_records


@pytest.fixture(scope='session')
def run_records():
    run_records = RunRecords(db_path=TEST_PATH)
    reset_database(run_records)
    return run_records


@pytest.fixture
def participant_records():
    pass


@pytest.fixture
def registration_records():
    pass


@pytest.fixture
def tag_records():
    pass


@pytest.fixture
def alignment_records():
    pass


@pytest.fixture(scope='session')
def model_records():
    model_records = ModelRecords(db_path=TEST_PATH)
    reset_database(model_records)
    return model_records


@pytest.fixture
def mlf_records():
    pass


@pytest.fixture(scope='session')
def validation_env(model_records):
    val_records = ValidationRecords(db_path=TEST_PATH)
    reset_database(val_records)

    (collab_id, project_id, expt_id, run_id, participant_id
    ) = generate_federated_combination()

    # Generate Upstream hierarchy
    # model_records = ModelRecords(db_path=TEST_PATH)
    model_records.create(
        collab_id=collab_id,
        project_id=project_id,
        expt_id=expt_id,
        run_id=run_id,
        details=generate_model_info()
    )

    # Simulate data
    action = random.choice(["classify", "regress"])
    validation_details = generate_inference_info(action, 10, "evaluate") 
    validation_updates = generate_inference_info(action, 10, "evaluate") 

    return (
        val_records, 
        validation_details,
        validation_updates,
        (collab_id, project_id, expt_id, run_id, participant_id)
    )



@pytest.fixture
def prediction_records():
    pass



if __name__ == "__main__":
    print(simulate_setup())
    print(generate_inference_info("classify", label_count=10, meta="evaluate"))
    print(generate_inference_info("regress", label_count=1, meta="predict"))
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
import tinydb

# Custom
from synarchive.connection import (
    CollaborationRecords, ProjectRecords, ExperimentRecords, RunRecords,
    ParticipantRecords, RegistrationRecords, TagRecords
)
from synarchive.training import AlignmentRecords, ModelRecords
from synarchive.evaluation import MLFRecords, ValidationRecords, PredictionRecords

##################
# Configurations #
##################

DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 
    "simulated_database.json"
)

TTP_SUBJECTS = [
    "Collaboration", 
    "Project", "Experiment", "Run", "Model", "Validation", "Prediction",
    "Registration", "Tag", "Alignment" 
]

WORKER_SUBJECTS = [
    "Participant",
    "Registration", "Tag", "Alignment"
]

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

KEY_ID_MAPPINGS = {
    # TTP-orchestrated mappings
    'collaboration': [
        ID_KEYS[r_type] 
        for r_type in ["collaboration"]
    ],
    'project': [
        ID_KEYS[r_type] 
        for r_type in ["collaboration", "project"]
    ],
    'experiment': [
        ID_KEYS[r_type] 
        for r_type in ["collaboration", "project", "experiment"]
    ],
    'run': [
        ID_KEYS[r_type] 
        for r_type in ["collaboration", "project", "experiment", "run"]
    ],
    'model': [
        ID_KEYS[r_type] 
        for r_type in ["collaboration", "project", "experiment", "run"]
    ],
    'validation': [
        ID_KEYS[r_type] 
        for r_type in ["participant", "collaboration", "project", "experiment", "run"]
    ],
    'prediction': [
        ID_KEYS[r_type] 
        for r_type in ["participant", "collaboration", "project", "experiment", "run"]
    ],
    
    # Worker-orchestrated mappings
    'participant': [
        ID_KEYS[r_type] 
        for r_type in ["participant"]
    ],
    'registration': [
        ID_KEYS[r_type] 
        for r_type in ["participant", "collaboration", "project"]
    ],
    'tag': [
        ID_KEYS[r_type] 
        for r_type in ["participant", "collaboration", "project"]
    ],
    'alignment':[
        ID_KEYS[r_type] 
        for r_type in ["participant", "collaboration", "project"]
    ],
}

LINK_MAPPINGS = {
    # TTP-orchestrated mappings
    'model': [],
    'validation': [],   # no need to delete predictions if deleted
    'prediction': [],   # no need to delete validations if deleted
    
    # Worker-orchestrated mappings
    'registration': [],
    'tag': ["Registration"],
    'alignment': ["Registration", "Tag"]
}

LINK_ID_MAPPINGS = {
    r_type: (
        [ID_KEYS[r_type]] +
        [ID_KEYS[subject.lower()] for subject in links]
    )
    for r_type, links in LINK_MAPPINGS.items()  
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

MODULE_MAPPINGS = {
    # Orchestrator-dependent
    'collaboration': CollaborationRecords,
    'project': ProjectRecords,
    'experiment': ExperimentRecords,
    'run': RunRecords,
    'model': ModelRecords,
    'validation': ValidationRecords,
    'prediction': PredictionRecords,

    # Participant-dependent
    'participant': ParticipantRecords,
    'registration': RegistrationRecords,
    'tag': TagRecords,
    'alignment': AlignmentRecords,

    # Misc
    "mlflow": MLFRecords
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


def simulate_port() -> int:
    return random.randint(1001, 65535)


def simulate_setup(
    collabs: int = 2,
    projects: int = 2,
    experiments: int = 2,
    runs: int = 3,
    participants: int = 5
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
    for participant_idx in range(participants):
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


def generate_collaboration_info():
    """
    """
    return {
        # Catalogue Connection
        'catalogue_host': simulate_ip(),
        'catalogue_port': simulate_port(),
        # Logger Connection
        'logger_host': simulate_ip(),
        'logger_ports': {
            'sysmetrics': simulate_port(),
            'director': simulate_port(),
            'ttp': simulate_port(),
            'worker': simulate_port(),
        },
        # Meter Connection
        'meter_host': simulate_ip(),
        'meter_port': simulate_port(),
        # MLOps Connection
        'mlops_host': simulate_ip(),
        'mlops_port': simulate_port(),
        # MQ Connection
        'mq_host': simulate_ip(),
        'mq_port': simulate_port(),
        # UI Connection
        'ui_host': simulate_ip(),
        'ui_port': simulate_port()
    }


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
        "f_port": simulate_port(),
        "port": simulate_port(),
        "log_msgs": simulate_choice(),
        "verbose": simulate_choice()
    }


def generate_registration_info():
    """
    """
    grid_count = random.randint(1, 10)
    channels = {
        f"node_{grid_idx}": {'host': simulate_ip(), 'port':simulate_port()} 
        for grid_idx in range(grid_count)
    }
    return {"role": "guest", **channels}


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
        X_alignments = sorted([
            random.randint(0, 1000) 
            for _ in range(random.randint(1, 100))
        ])
        y_alignments = sorted([
            random.randint(0, 10) 
            for _ in range(random.randint(1, 10))
        ])
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


def generate_database(
    collabs: int = 2,
    projects: int = 2,
    experiments: int = 2,
    runs: int = 3,
    participants: int = 5,
    db_path: str = DB_PATH
) -> str:
    """
    """
    CORE_GENERATOR_MAPPINGS = {
        'collaboration': generate_collaboration_info,
        'project': generate_project_info,
        'experiment': generate_experiment_info,
        'run': generate_run_info,
        'participant': generate_participant_info,
    }

    all_simulated_keys = simulate_setup(
        collabs=collabs,
        projects=projects,
        experiments=experiments,
        runs=runs,
        participants=participants
    )

    participant_keys = all_simulated_keys['participant']
    project_keys = all_simulated_keys['project']
    expt_keys = all_simulated_keys['experiment']
    run_keys = all_simulated_keys['run']
    

    # Simulate core archives
    for r_type, r_keys in all_simulated_keys.items():
        archive = MODULE_MAPPINGS[r_type](db_path)
        for r_key in r_keys:
            generator = CORE_GENERATOR_MAPPINGS[r_type]
            if r_type != "participant":
                simulated_details = generator()
            else:
                simulated_details = {
                    'id': r_key['participant_id'],
                    **generator()
                }
            archive.create(**r_key, details=simulated_details)

    # Simulate participant-associated archives
    for participant_key in participant_keys:
        for project_key in project_keys:
            
            # Simulate a registration entry
            reg_assoc_archive = MODULE_MAPPINGS["registration"](db_path)
            reg_assoc_archive.create(
                **project_key,
                **participant_key,
                details=generate_registration_info()
            )
                
            # Simulate a tag entry
            tag_assoc_archive = MODULE_MAPPINGS["tag"](db_path)
            tag_assoc_archive.create(
                **project_key,
                **participant_key,
                details=generate_tag_info()
            )

            # Simulate an alignment entry
            align_assoc_archive = MODULE_MAPPINGS["alignment"](db_path)
            align_assoc_archive.create(
                **project_key,
                **participant_key,
                details=generate_alignment_info()
            )

    # Simulate result archives
    for run_key in run_keys:

        # Simulate model generation
        model_assoc_archive = MODULE_MAPPINGS["model"](db_path)
        model_assoc_archive.create(**run_key, details=generate_model_info())

        action = random.choice(["classify", "regress"])
        label_count = random.randint(1, 10) if action == "classify" else 1
        for participant_key in participant_keys:

            # Simulate validation generation
            val_assoc_archive = MODULE_MAPPINGS["validation"](db_path)
            val_assoc_archive.create(
                **run_key, 
                **participant_key,
                details=generate_inference_info(
                    action=action, 
                    label_count=label_count, 
                    meta="evaluate"
                )
            )

            # Simulate prediction generation
            pred_assoc_archive = MODULE_MAPPINGS["prediction"](db_path)
            pred_assoc_archive.create(
                **run_key, 
                **participant_key,
                details=generate_inference_info(
                    action=action, 
                    label_count=label_count, 
                    meta="predict"
                )
            )

    total_collabs = len(MODULE_MAPPINGS['collaboration'](db_path).read_all())
    total_projects = len(MODULE_MAPPINGS['project'](db_path).read_all())
    total_expts = len(MODULE_MAPPINGS['experiment'](db_path).read_all())
    total_runs = len(MODULE_MAPPINGS['run'](db_path).read_all())
    total_models = len(MODULE_MAPPINGS['model'](db_path).read_all())
    total_validations = len(MODULE_MAPPINGS['validation'](db_path).read_all())
    total_predictions = len(MODULE_MAPPINGS['prediction'](db_path).read_all())
    total_participants = len(MODULE_MAPPINGS['participant'](db_path).read_all())
    total_registrations = len(MODULE_MAPPINGS['registration'](db_path).read_all())
    total_tags = len(MODULE_MAPPINGS['tag'](db_path).read_all())
    total_alignments = len(MODULE_MAPPINGS['alignment'](db_path).read_all())

    # Check if core archives were generated properly
    assert total_collabs == collabs
    assert total_projects == (collabs * projects)
    assert total_expts == (collabs * projects * experiments)
    assert total_runs == (collabs * projects * experiments * runs)
    assert total_participants == participants

    # Check if participant-associated archives were generated properly
    assert total_registrations == (collabs * projects * participants)
    assert total_tags == (collabs * projects * participants)
    assert total_alignments == (collabs * projects * participants)

    # Check if result archives were generated properly
    assert total_models == (collabs * projects * experiments * runs)
    assert total_validations == (collabs * projects * experiments * runs * participants)
    assert total_predictions == (collabs * projects * experiments * runs * participants)
    
    return db_path

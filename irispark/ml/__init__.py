from __future__ import annotations

from .automl import AutoMLClassifier, AutoMLModel, AutoMLRegressor
from .base import Estimator, Model, Transformer
from .classification import LogisticRegression, LogisticRegressionModel
from .custom import CustomModelClassifier, CustomModelModel
from .ensemble import (
    KNeighborsClassifier,
    KNeighborsRegressor,
    RandomForestClassifier,
    RandomForestRegressor,
)
from .evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from .feature import (
    Binarizer,
    Imputer,
    ImputerModel,
    IndexToString,
    MaxAbsScaler,
    MaxAbsScalerModel,
    MinMaxScaler,
    MinMaxScalerModel,
    OneHotEncoder,
    OneHotEncoderModel,
    QuantileDiscretizer,
    QuantileDiscretizerModel,
    SQLTransformer,
    StandardScaler,
    StandardScalerModel,
    StringIndexer,
    StringIndexerModel,
    VectorAssembler,
)
from .linalg import LogicalVector
from .param import Param, Params, TypeConverters
from .persistence import delete_model, list_models, load, load_by_name, save
from .pipeline import Pipeline, PipelineModel
from .planner import BackendCapability, BackendType, MLSemanticPlanner
from .regression import LinearRegression, LinearRegressionModel
from .tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit

__all__ = [
    # Core framework
    "Transformer",
    "Estimator",
    "Model",
    "Pipeline",
    "PipelineModel",
    "Param",
    "Params",
    "TypeConverters",
    "LogicalVector",
    "MLSemanticPlanner",
    "BackendType",
    "BackendCapability",
    # Feature transformers
    "VectorAssembler",
    "StringIndexer",
    "StringIndexerModel",
    "OneHotEncoder",
    "OneHotEncoderModel",
    "StandardScaler",
    "StandardScalerModel",
    "QuantileDiscretizer",
    "QuantileDiscretizerModel",
    "Imputer",
    "ImputerModel",
    "Binarizer",
    "MinMaxScaler",
    "MinMaxScalerModel",
    "MaxAbsScaler",
    "MaxAbsScalerModel",
    "IndexToString",
    "SQLTransformer",
    # Supervised estimators
    "LinearRegression",
    "LinearRegressionModel",
    "LogisticRegression",
    "LogisticRegressionModel",
    # Ensemble (EPython/sklearn backend)
    "RandomForestClassifier",
    "RandomForestRegressor",
    "KNeighborsClassifier",
    "KNeighborsRegressor",
    # AutoML Custom Models
    "CustomModelClassifier",
    "CustomModelModel",
    # Tuning
    "ParamGridBuilder",
    "CrossValidator",
    "TrainValidationSplit",
    # Persistence
    "save",
    "load",
    "load_by_name",
    "list_models",
    "delete_model",
    # Evaluation
    "RegressionEvaluator",
    "BinaryClassificationEvaluator",
    # IntegratedML AutoML (IRIS extension)
    "AutoMLClassifier",
    "AutoMLRegressor",
    "AutoMLModel",
]

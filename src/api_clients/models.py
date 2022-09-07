import logging
import os
import re
from typing import List, Dict

import gensim
import numpy as np


class Doc2Vec:
    def __init__(self, model: gensim.models.Doc2Vec, min_words: int = 250):
        self.model = model
        self.min_words = min_words

    def preprocess(self, document: str) -> List[str]:
        """Pre-process document."""
        return gensim.utils.simple_preprocess(document)

    def infer(self, document: str) -> List[float]:
        q = self.preprocess(document)
        if len(q) < self.min_words:
            exception = ValueError(f"document length ({len(q)} words) is less than the minimum of {self.min_words} words")

        np_vector: np.ndarray = self.model.infer_vector(q)
        return np_vector.tolist()


MODEL_RE = re.compile(r"(?P<model_type>[\w-]+)_(?P<model_id>[^.]+).model$")
MODEL_PREFIXES = {"doc2vec", "doc2vec-incremental"}


def from_file(file_path):
    """Given a `file_path`, return the match object and instantiated object for the model.
    `file_path` is a string path of the form /abc/doc2vec_12345.model.

    Returns `None` if no match."""

    # Extract the filename from the path, prevents directories containing _ from matching
    file_name = file_path.split('/')[-1]
    match = MODEL_RE.search(file_name)
    if match and match['model_type'] in MODEL_PREFIXES:
        model = gensim.models.Doc2Vec.load(file_path)
        return match, Doc2Vec(model)


def load_dir(model_dir: str) -> Doc2Vec:
    """
    Load all models in `model_dir`, returning a dictionary whose keys are the model_id and values are the corresponding
    Doc2Vec object."""
    dir_files = os.listdir(model_dir)
    logging.info("model_dir (%s) contains %d files", model_dir, len(dir_files))
    if len(dir_files) == 0:
        raise ValueError("%s contains no files!" % model_dir)
    files = filter(None, [from_file(os.path.join(model_dir, file)) for file in dir_files])
    _, model = next(files)
    return model

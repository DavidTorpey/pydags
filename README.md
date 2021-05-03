# pydags

**pydags** is a Python package to facilitate the creation and running of
lightweight DAG-based workloads locally. Whereas technologies like Airflow,
Kubeflow, and Luigi are more heavyweight, enterprise-level workflow managers,
**pydags** is instead an extensible, simple, lightweight alternative tailored
for local development and execution. There are no dependencies on Docker,
Kubernetes, or any other such technologies, and it is use-case agnostic (unlike
Kubeflow/Luigi).

## Terminology

### Stage

A *Stage* is synonymous to a node in a DAG.

### Pipeline

A *Pipeline* is synonymous to a DAG, and is comprised of 1 or more *Stages* in
a DAG-like structure.

## Get Started

### External Dependencies

**pydags** requires multiple non-Python dependencies in order to function properly.
These include Redis (for internal operation) and GraphViz (for visualization). To install these, run the following command in terminal:

```bash
sudo apt-get install redis graphviz
```

Installing redis with this command on most *nix systems will result in the
Redis server starting automatically. You can verify this by running the
`redis-server` command, which should result in an `Address already in use`
message, or similar.

### Python Dependencies

The requirements for the SDK are defined in the `requirements.txt` file, and
can be installed with the following command:

```bash
pip install -r requirements.txt
```

### Install pydags

Currently, pydags needs to be installed from source. Simply run the following command:

```bash
python3 setup.py install
```

You may require `sudo` permissions to run this command. If you do not have
`sudo` permissions on your system, then you can point to a different
installation directory using the `--home /path/to/folder/` flag and point it
to a folder you have write permissions to. Ensure that the `build/libs`
directory  that gets created is in your `PYTHONPATH` environment variable.

## Example Usage

### Expressing a DAG

Below is a simple example that aims to simply demonstrate how to specify DAGs
in **pydags**. In this case, each stage of the pipeline is a Python function
decorated with the `@stage` decorator.

```python
from pydags.pipeline import Pipeline
from pydags.stage import stage


@stage
def stage_1():
    print('Running stage 1')

@stage
def stage_2():
    print('Running stage 2')

@stage
def stage_3():
    print('Running stage 3')

@stage
def stage_4():
    print('Running stage 4')

@stage
def stage_5():
    print('Running stage 5')

@stage
def stage_6():
    print('Running stage 6')

def build_pipeline():
    stage1 = stage_1()
    stage2 = stage_2()
    stage3 = stage_3().after(stage2)
    stage4 = stage_4().after(stage2)
    stage5 = stage_5().after(stage1)
    stage6 = stage_6().after(stage3).after(stage4).after(stage5)

    pipeline = Pipeline()

    pipeline.add_stages([
        stage1, stage2, stage3,
        stage4, stage5, stage6
    ])
    
    return pipeline


pipeline = build_pipeline()

pipeline.visualize()

pipeline.start()
```

Stages of the pipeline that can be run in parallel (in the above case, stages 1
and 2, and stages 3, 4, 5) will only be run in parallel is you set the
`num_cores` argument of the `.start()` method to a positive integer
(representing the number of cores to distribute computation across). For
example, if you want to parallelize the execution of such nodes that can be run
in parallel, then simply replace `pipeline.start()` with
`pipeline.start(num_cores=8)` (to use 8 cores).

### A Simple ML Pipeline

Below is an example of a simple ML pipeline consisting of 3 stages: 1) data
download, 2) preprocessing, 3) model training. All 3 stages are subclasses of
`RedisStage`, and thus inherit the functionality to read from and write to a
Redis server. The data is thus passed between the stages using Redis. You may
subclass DiskCacheStage, or implement your own cache/storage backend, if Redis
is not suited to your use case.

Please note that you will need to install the additional `scikit-learn`
dependency to run this example.

```python
import redis
from sklearn.datasets import load_wine
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

from pydags.pipeline import Pipeline
from pydags.stage import RedisStage


class DataIngestor(RedisStage):
    @staticmethod
    def download_data():
        data = load_wine()
        features = data['data']
        targets = data['target']
        return features, targets

    def run(self, *args, **kwargs):
        features, targets = self.download_data()
        self.write('features', features)
        self.write('targets', targets)


class DataPreprocessor(RedisStage):
    @staticmethod
    def normalize(features):
        return MinMaxScaler().fit_transform(features)

    @staticmethod
    def split(features, targets):
        return train_test_split(features, targets, test_size=0.2)

    def run(self, *args, **kwargs):
        features = self.read('features')
        targets = self.read('targets')

        xtr, xte, ytr, yte = self.split(features, targets)

        xtr = self.normalize(xtr)
        xte = self.normalize(xte)

        data = {
            'xtr': xtr, 'xte': xte,
            'ytr': ytr, 'yte': yte
        }

        self.write('preprocessed_data', data)


class ModelTrainer(RedisStage):
    def __init__(self, *args, **kwargs):
        super(ModelTrainer, self).__init__(*args, **kwargs)

        self.model = None

    def train_model(self, xtr, ytr):
        self.model = RandomForestClassifier().fit(xtr, ytr)

    def test_model(self, xte, yte):
        acc = self.model.score(xte, yte)
        return acc

    def run(self, *args, **kwargs):
        preprocessed_data = self.read('preprocessed_data')

        xtr = preprocessed_data['xtr']
        xte = preprocessed_data['xte']
        ytr = preprocessed_data['ytr']
        yte = preprocessed_data['yte']

        self.train_model(xtr, ytr)

        acc = self.test_model(xte, yte)

        print('Accuracy:', acc)


def build_pipeline():
    redis_instance = redis.Redis(host='localhost', port=6379, db=0)

    data_ingestor = DataIngestor(redis_instance=redis_instance)

    data_preprocessor = DataPreprocessor(redis_instance=redis_instance).after(data_ingestor)

    model_trainer = ModelTrainer(redis_instance=redis_instance).after(data_preprocessor)

    pipeline = Pipeline()
    pipeline.add_stages([data_ingestor, data_preprocessor, model_trainer])

    return pipeline


p = build_pipeline()

p.visualize()

p.start()
```
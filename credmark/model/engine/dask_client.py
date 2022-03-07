import os
import sys
import zipfile
import webbrowser
import uuid
import inspect
from typing import Any, Dict, List
import tempfile

import importlib
import types


from typing import (
    Dict,
    Tuple,
    Any,
    List,
    Callable,
    TypedDict,
    Optional,
    Union,
)

from web3 import HTTPProvider, Web3

import dask.distributed as dask_dist

from dask.optimization import (
    cull,
    inline,
    inline_functions,
    fuse,
)

from credmark.model.errors import ModelRunError
from credmark.types.data.block_number import BlockNumber


class DaskResult(TypedDict):
    result: Any
    dsk: dict
    deps: list
    futures: Optional[list]


def reload_models(mod):
    mod = importlib.reload(mod)
    reloaded = []
    for k, v in mod.__dict__.items():
        if isinstance(v, types.ModuleType):
            setattr(mod, k, importlib.import_module(v.__name__))
            reloaded.append((k, v))
    return reloaded


class DaskClient():
    """
    A class provides launch and/or connect to a Dask Host.
    """

    def __init__(self,
                 cluster: str,
                 web3_http_provider: str,
                 block_number: BlockNumber,
                 open_browser: bool = False,
                 model_paths: List[str] = []
                 ):

        if cluster == 'sequence':
            client = 'sequence'
            dashboard_link = None
        elif cluster.startswith('localhost:'):
            n_workers = int(cluster[cluster.index(':') + 1:])
            threads_per_worker = 1

            # launch a local server
            client = dask_dist.Client(
                n_workers=n_workers, threads_per_worker=threads_per_worker, set_as_default=False,)
            print(f'Launched local cluster with dashboard at {client.dashboard_link}')
            dashboard_link = client.dashboard_link
        elif cluster.startswith('tcp://'):
            # connect to a server
            client = dask_dist.Client(address=cluster, set_as_default=False,)
            print(
                f'Connected to cluster at {cluster} with dashboard at {client.dashboard_link}')
            self.upload_mod(model_paths)
            dashboard_link = client.dashboard_link
        else:
            raise ModelRunError(f'Unrecognizable cluster setting = {cluster}')

        self.__client = client
        if dashboard_link is not None and open_browser:
            webbrowser.open(dashboard_link, new=1)

    def upload_mod(self, mod_paths):
        def everything(s):
            # print(s)
            return True

        fp = tempfile.NamedTemporaryFile('w', suffix='.zip', prefix='models_pkg_')
        fp.close()

        with zipfile.PyZipFile(fp.name, mode="w", optimize=2) as zip_pkg:
            for mod_dir in mod_paths:
                all_init_files = []
                fp_init = os.path.join(mod_dir, '__init__.py')
                if not os.path.isfile(fp_init):
                    with open(fp_init, 'w') as f:
                        f.write('')
                        all_init_files.append(fp_init)
                for root, dirs, files in os.walk(mod_dir):
                    for name in dirs:
                        if name != '__pycache__':
                            fp_init = os.path.join(root, name, '__init__.py')
                            if not os.path.isfile(fp_init):
                                with open(fp_init, 'w') as f:
                                    f.write('')
                                    all_init_files.append(fp_init)

                zip_pkg.writepy(mod_dir, filterfunc=everything)

        self.client.upload_file(fp.name)
        self.client.submit(lambda x: sys.path, 0).result()
        for mod_dir in mod_paths:
            mod = importlib.import_module(mod_dir)
            self.client.submit(lambda x, mod=mod: reload_models(mod), 1).result()
        os.remove(fp.name)

    @property
    def client(self) -> dask_dist.Client:
        if isinstance(self.__client, dask_dist.Client):
            return self.__client
        else:
            raise ModelRunError('Apply dask client operation to local sequence client')

    def dashboard_link(self):
        return self.client.dashboard_link

    def clear_all(self):
        # clean all futures
        while len(self.client.who_has()) > 0:
            for k in self.client.who_has().keys():
                dask_dist.Future(k, client=self.client).cancel()

    def run_graph(self, dsk: Dict[str, Tuple], outputs: List[Any], inline_funcs: List[Callable] = [], clear=True) -> DaskResult:
        assert type(dsk) == dict
        assert type(outputs) == list

        try:
            dsk_opt1, dsk_deps1 = cull(dsk, keys=outputs)
        except KeyError as e:
            raise ValueError(f'[cull] Found undefined output {e} in the graph {dsk}')

        try:
            dsk_opt2 = inline(dsk_opt1, keys=outputs, dependencies=dsk_deps1)
        except Exception as e:
            raise ValueError(f'[inline]: {e}')

        if inline_funcs is None or len(inline_funcs) == 0:
            try:
                dsk_opt3 = inline_functions(dsk_opt2, output=outputs, fast_functions=inline_funcs,  # [len, str.split]
                                            dependencies=dsk_deps1)
            except Exception as e:
                raise ValueError(f'[inline_functions]: {e}')
        else:
            dsk_opt3 = dsk_opt2

        try:
            dsk_opt4, dsk_deps4 = fuse(dsk_opt3, keys=outputs, dependencies=dsk_deps1)
        except:
            raise ValueError(f'[fuse]: {e}')

        if isinstance(self.client, str):
            res_dict = {'result': 'TODO'}
            return {'result': res_dict, 'dsk': dsk_opt4, 'deps': dsk_deps4}
        else:
            try:
                res_future = self.client.get(dsk_opt4, outputs, sync=False)
                res_dict = {k: v.result() for k, v in zip(outputs, res_future)}
            finally:
                if clear:
                    # client can cache results with the same name. Use cancel to clear the calculations.
                    # [ v.cancel() for k, v in zip(outputs, res_future)]
                    return {'result': res_dict, 'dsk': dsk_opt4, 'deps': dsk_deps4}
                else:
                    return {'result': res_dict, 'dsk': dsk_opt4, 'deps': dsk_deps4, 'futures': res_future}

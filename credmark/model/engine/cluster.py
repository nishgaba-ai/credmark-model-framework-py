import webbrowser
from typing import Any, Dict, List
import tempfile
import zipfile

from typing import (
    Dict,
    Tuple,
    Any,
    List,
    Callable,
    TypedDict,
    Optional,
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

from credmark.types.data.address import Address


class DaskResult(TypedDict):
    result: Any
    dsk: dict
    deps: list
    futures: Optional[list]


class Cluster():
    """
    A class provides launch and/or connect to a Dask Host.
    """

    def __init__(self,
                 web3_http_provider: str,
                 block_number: int,
                 cluster: str,
                 model_paths: List[str],
                 open_browser=False
                 ):
        threads_per_worker = 1
        if cluster == 'sequence':
            client = 'sequence'
            print('Use sequence cluster')
            dashboard_link = None
        elif cluster.startswith('tcp://'):
            client = dask_dist.Client(address=cluster, set_as_default=False,)
            print(f'Connected to cluster at {cluster} with dashboard at {client.dashboard_link}')
            dashboard_link = client.dashboard_link
            breakpoint()
            self.upload_mods(model_paths)
        elif cluster.startswith('localhost:'):
            n_workers = int(cluster[cluster.index(':') + 1:])
            client = dask_dist.Client(
                n_workers=n_workers, threads_per_worker=threads_per_worker, set_as_default=False,)
            print(f'Launched local cluster with dashboard at {client.dashboard_link}')
            dashboard_link = client.dashboard_link
        else:
            raise ModelRunError(f'Invalid cluster setup config = {cluster}')

        self.__client = client
        if open_browser and dashboard_link is not None:
            webbrowser.open(dashboard_link, new=1)

        self.web3_http_provider = web3_http_provider
        self.block_number = block_number

    def upload_mods(self, mod_paths):
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
    def client(self):
        if isinstance(self.__client, str):
            raise ModelRunError('Use sequence cluster now. There no cluster client method')
        else:
            return self.__client

    def init_web3(self, force=False):
        worker = dask_dist.get_worker()
        http_provider = self.web3_http_provider
        block_number = self.block_number
        with worker._lock:
            if not hasattr(worker, "_web3"):
                worker._web3 = {}
                has_web3 = False
            else:
                has_web3 = (http_provider in worker._web3 and
                            block_number in worker._web3[http_provider] and
                            not force)
            if not has_web3:
                web3 = Web3(HTTPProvider(http_provider))
                web3.eth.default_block = block_number if \
                    block_number is not None else 'latest'
                worker._web3[http_provider] = {block_number: {'web3': web3}}
                return True
            else:
                return False

    def create_contract(self, contract_address: Address, contract_abi: str, force=False):
        worker = dask_dist.get_worker()
        http_provider = self.web3_http_provider
        block_number = self.block_number
        with worker._lock:
            web3_dict = worker._web3[http_provider][block_number]
            web3 = web3_dict['web3']
            if contract_address not in web3_dict or force:
                contract = web3.eth.contract(
                    address=contract_address.checksum,
                    abi=contract_abi)
                worker._web3[http_provider][block_number][contract_address] = contract
                return True
            else:
                return False

    def get_contract(self, contract_address: Address, contract_abi: str):
        worker = dask_dist.get_worker()
        self.init_web3()
        self.create_contract(contract_address, contract_abi)
        http_provider = self.web3_http_provider
        block_number = self.block_number
        with worker._lock:
            contract = worker._web3[http_provider][block_number][contract_address]
            return contract

    def get_contract_function(self, contract_address: Address, contract_abi: str, func_name: str):
        worker = dask_dist.get_worker()
        contract = self.get_contract(contract_address, contract_abi)
        with worker._lock:
            func = contract[func_name]
            return func

    def contract_function_call(self, contract_address: Address, contract_abi: str, func_name: str, *param):
        worker = dask_dist.get_worker()
        contract_func = self.get_contract_function(contract_address, contract_abi, func_name)
        with worker._lock:
            result = contract_func(*param).call()
            return result

    def clear_all(self):
        # clean all futures
        while len(self.client.who_has()) > 0:
            for k in self.__client.who_has().keys():
                dask_dist.Future(k, client=self.__client).cancel()

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
        except Exception as err:
            raise ValueError(f'[fuse]: {err}')

        if self.client == 'sequence':
            return {'result': {'result': 'TODO'}, 'dsk': dsk, 'deps': dsk, 'futures': []}
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

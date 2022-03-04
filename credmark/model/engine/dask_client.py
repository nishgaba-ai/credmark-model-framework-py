import webbrowser
import uuid
import inspect
from typing import Any, Dict, List

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

from dask.distributed import ( # # pylint disable=unused-imports
    get_worker,
    get_client,
)

from dask.optimization import (
    cull,
    inline,
    inline_functions,
    fuse,
)

from credmark.types.data.address import Address


class DaskResult(TypedDict):
    result: str
    dsk: dict
    deps: list
    futures: Optional[list]

class DaskClient():
    """
    A class provides launch and/or connect to a Dask Host.
    """

    def __init__(self,
                 address=None,
                 n_workers=10,
                 threads_per_worker=1,
                 open_browser=False,
                 ):
        if address is not None:
            client = dask_dist.Client(address=address, set_as_default=False,)
            print(f'Connected to cluster at {address} with dashboard at {client.dashboard_link}')
        else:
            client = dask_dist.Client(n_workers=n_workers, threads_per_worker=threads_per_worker, set_as_default=False,)
            print(f'Launched local cluster with dashboard at {client.dashboard_link}')

        self.__client = client
        if open_browser:
            webbrowser.open(self.__client.dashboard_link, new=1)

    def get_client(self):
        return self.__client

    def has_what(self):
        self.__client.has_what()

    def who_has(self):
        self.__client.who_has()

    def dashboard_link(self):
        return self.__client.dashboard_link

    def clear_all(self):
        # clean all futures
        while len(self.__client.who_has()) > 0:
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
                dsk_opt3 = inline_functions(dsk_opt2, output=outputs, fast_functions=inline_funcs, # [len, str.split]
                                            dependencies=dsk_deps1)
            except Exception as e:
                raise ValueError(f'[inline_functions]: {e}')
        else:
            dsk_opt3 = dsk_opt2

        try:
            dsk_opt4, dsk_deps4 = fuse(dsk_opt3, keys=outputs, dependencies=dsk_deps1)
        except:
            raise ValueError(f'[fuse]: {e}')

        try:
            res_future = self.__client.get(dsk_opt4, outputs, sync=False)
            res_dict = { k:v.result() for k, v in zip(outputs, res_future) }
        finally:
            if clear:
                # client can cache results with the same name. Use cancel to clear the calculations.
                # [ v.cancel() for k, v in zip(outputs, res_future)]
                return { 'result': res_dict, 'dsk': dsk_opt4, 'deps': dsk_deps4 }
            else:
                return { 'result': res_dict, 'dsk': dsk_opt4, 'deps': dsk_deps4, 'futures': res_future}

# n means it depends on the last n-task to finish but not get input
def call(f, *args, **kwargs):
    return f(*args, **kwargs)

def depend_on(f, n, *args, **kwargs):
    assert n > -1
    if n == 0:
        return call(f, *args, **kwargs)
    else:
        return f(*args[:(-n)], **kwargs)

def depend_all(*_args, **_kwargs):
    return depend_on(lambda:True,len(_args)+len(_kwargs))

class ModelTask():
    # TODO
    pass

class Task():
    """
    input is a tuple of value input, task inputs
    """
    def __init__(self, name, f, input: Tuple[List[Any],List[str]] = ([],[])):
        self.task_name = name
        self.uuid_name = f.__name__ + '-' + str(uuid.uuid4())
        self._f = f
        self._args, self._deps = input
        f_args = inspect.getfullargspec(f).args
        f_args_defaults = inspect.getfullargspec(f).defaults
        deduct = 1 if inspect.ismethod(f) else 0 + 0 if f_args_defaults is None else len(f_args_defaults)

        assert isinstance(self._args, list) and isinstance(self._deps, list)
        if len(f_args) - deduct > len(self._args)+len(self._deps):
            raise ValueError(f'Input function\'s required arguments ({len(f_args), f_args}) is longer than the input ({len(input), input})')
        self._f_args = f_args

    def __call__(self):
        if len(self._deps) == 0:
            return (call, self._f, *self._args)
        else:
            return (depend_on, self._f, len(self._args)+len(self._deps)-len(self._f_args), *self._args, *[t.uuid_name for t in self._deps])

class Pipe():
    def __init__(self, *ts):
        self._graph = {}
        self._tasks = []
        self._task_names = {}
        self._uuid_names = {}
        self.extend(ts)

    def add(self, t):
        self.extend([t])

    def extend(self, ts):
        self._tasks.extend(ts)
        for t in ts:
            if t.task_name in self._uuid_names:
                raise ValueError(f'There is already a task of the same name {t.task_name} in the pipe.')
            self._uuid_names[t.task_name] = t.uuid_name
            self._task_names[t.uuid_name] = t.task_name
            self._graph[t.uuid_name] = t()
        # TODO: check for DAG for the graph

    def graph(self):
        return self._graph

    def run(self, client, output):
        new_output = [self._uuid_names[v] for v in output]
        ret = client.run_graph(self._graph, new_output)
        res_dict = ret['result']
        new_dict = { (self._task_names.get(uuid_name, uuid_name)):v for (uuid_name, v) in res_dict.items()}
        return new_dict

    def run_plain(self, client, output):
        new_output = [self._uuid_names[v] for v in output]
        ret = client.get(self._graph, new_output)
        res_dict = ret['result']
        new_dict = { (self._task_names.get(uuid_name, uuid_name)):v for (uuid_name, v) in res_dict.items()}
        return new_dict



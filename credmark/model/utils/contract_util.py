from credmark.types import Contract, Address
import credmark.model

from typing import (
    Union,
    List,
)

import dask.distributed as dask_dist

from web3 import Web3

class ContractUtil:

    def __init__(self,
                 context,
                 ) -> None:
        self.context: credmark.model.ModelContext = context

    def load_description(self,
                         address: Union[Address, None] = None,
                         name: Union[str, None] = None,
                         protocol: Union[str, None] = None,
                         product: Union[str, None] = None,
                         abi: Union[dict, None] = None,
                         tags: Union[dict, None] = None) -> List[Contract]:
        if name is None and address is None and abi is None:
            raise Exception

            # This means we can end up with different KINDS of contracts together.
            # probably no bueno
            # we could do it if we could return a contract that is a subclass of web3.contract.Contract
            # but I don't understand how to do that with a web3 context

        contracts: List[Contract] = []
        q = {}

        if address is not None:
            q["contractAddress"] = address
        if name is not None:
            q["contractName"] = name
        if protocol is not None:
            q["protocol"] = protocol
        if product is not None:
            q["product"] = product
        if tags is not None:
            q["tags"] = tags
        if abi is not None:
            q["abi"] = abi

        contract_q_results = self.context.run_model('contract.metadata', q)
        for contract in contract_q_results['contracts']:
            contracts.append(Contract(_instance=None, **contract))
        return contracts

    def load_address(self, address: str) -> Contract:
        contract_q_results = self.context.run_model(
            'contract.metadata', {'contractAddress': address})
        contract_obj = Contract(_instance=None,
                                **(contract_q_results['contracts'][0]))
        return contract_obj

    def init_web3(self, force=False):
        worker = dask_dist.get_worker()
        http_provider = self.context.web3_http_provider
        block_number = self.context.block_number
        with worker._lock:
            if not hasattr(worker, "_web3"):
                worker._web3 = {}
                has_web3 = False
            else:
                has_web3 = ( http_provider in worker._web3 and
                             block_number in worker._web3[http_provider] and
                             not force )
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


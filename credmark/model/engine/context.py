import logging
import os

from typing import (
    Union,
    List,
)

from credmark.model.context import ModelContext
from credmark.model.errors import MaxModelRunDepthError, ModelRunError
from credmark.model.engine.model_api import ModelApi
from credmark.model.engine.model_loader import ModelLoader
from credmark.model.engine.pipe import Pipe
from credmark.model.web3 import Web3Registry
from credmark.types.dto import DTO


class EngineModelContext(ModelContext):
    """Model context class

    Instance attributes:
        chain_id (int): chain ID, ex 1
        block_number (int): default block number
        web3 (Web3): a configured web3 instance for RPC calls

    Methods:
        run_model(...) - run the specified model and return the results
    """

    logger = logging.getLogger(__name__)

    dev_mode = False
    max_run_depth = 20

    @classmethod
    def create_context_and_run_model(cls,
                                     chain_id: int,
                                     block_number: int,
                                     model_slug: str,
                                     model_version: Union[str, None] = None,
                                     input: Union[dict, None] = None,
                                     model_loader: Union[ModelLoader,
                                                         None] = None,
                                     chain_to_provider_url: Union[dict[str, str], None] = None,
                                     api_url: Union[str, None] = None,
                                     run_id: Union[str, None] = None,
                                     depth: int = 0,
                                     cluster: Union[str, None] = None):
        """
        Parameters:
            run_id (str | None): a string to identify a particular model run. It is
                same for any other models run from within a model.

        Raises:
            ModelRunError if model output is not a dict-like object.
            Exception on other errors
        """

        if model_loader is None:
            model_loader = ModelLoader(['.'])

        api_key = os.environ.get('CREDMARK_API_KEY')

        # If we have an api url or a key, we create the api
        # TODO: When public api is available, we will always create the api
        api = ModelApi(api_url, api_key) if api_url or api_key else None

        web3_registry = Web3Registry(chain_to_provider_url)

        context = EngineModelContext(
            chain_id, block_number, web3_registry, run_id, depth, model_loader, api, cluster)

        # We set the block_number in the context so we pass in
        # None for block_number to the run_model method.

        result_tuple = context._run_model(
            model_slug, input, None, model_version)

        output = result_tuple[2]
        output_as_dict = context.transform_data_for_dto(output, None, model_slug, 'output')

        response = {
            'slug': result_tuple[0],
            'version': result_tuple[1],
            'output': output_as_dict,
            'dependencies': context.__dependencies}
        return response

    def __init__(self,
                 chain_id: int,
                 block_number: int,
                 web3_registry: Web3Registry,
                 run_id: Union[str, None],
                 depth: int,
                 model_loader: ModelLoader,
                 api: Union[ModelApi, None],
                 cluster: Union[str, None]):
        super().__init__(chain_id, block_number, web3_registry, cluster, model_loader.model_paths)
        self.run_id = run_id
        self.__depth = depth
        self.__dependencies = {}
        self.__model_loader = model_loader
        self.__api = api

    @ property
    def dependencies(self):
        return self.__dependencies

    def _add_dependency(self, slug: str, version: str, count: int):
        versions = self.__dependencies.get(slug)
        if versions is None:
            self.__dependencies[slug] = {version: count}
        else:
            if version in versions:
                versions[version] += count
            else:
                versions[version] = count

    def _add_dependencies(self, dep_dict: dict):
        for slug, versions in dep_dict.items():
            if slug not in self.__dependencies:
                self.__dependencies[slug] = versions
            else:
                for version, count in versions.items():
                    self._add_dependency(slug, version, count)

    def run_pipe(self, pipe: Pipe, outputs: List[str]):
        return pipe.run(self.cluster, outputs)

    def run_model(self,
                  slug,
                  input=None,
                  return_type=None,
                  block_number=None,
                  version=None,
                  ):
        """Run a model by slug and optional version.

        Parameters:
            slug (str): the slug of the model
            input (dict | None): an optional dictionary of
                  input data that will be passed to the model when it is run.
            block_number (int | None): optional block number to use as context.
                  If None, the block_number of the current context will be used.
            version (str | None): optional version of the model.
                  If version is None, the latest version of
                  the model is used.
            return_type (DTO Type | None): optional class to use for the
                  returned output data. If not specified, returned value is a dict.
                  If a DTO specified, the returned value will be an instance
                  of that class if the output data is compatible with it. If its not,
                  an exception will be raised.

        Returns:
            The output returned by the model's run() method as a dict
            or a DTO instance if return_type is specified.

        Raises:
            MissingModelError if requested model is not available
            Exception on other errors
        """

        if block_number is not None and block_number > self.block_number:
            raise ModelRunError(
                f'Attempt to run model {slug} at context block {self.block_number} '
                f'with future block {block_number}')

        res_tuple = self._run_model(slug, input, block_number, version)

        # The last item of the tuple is the output.
        output = res_tuple[-1]
        return self.transform_data_for_dto(output, return_type, slug, 'output')

    def _run_model(self,
                   slug: str,
                   input: Union[dict, DTO, None],
                   block_number: Union[int, None],
                   version: Union[str, None]
                   ):

        api = self.__api

        # We raise an exception for missing class
        # if we have no api or this is a top-level run.
        model_class = self.__model_loader.get_model_class(
            slug, version, api is None or
            (self.__depth == 0 and not self.dev_mode))

        self.__depth += 1
        if self.__depth >= self.max_run_depth:
            raise MaxModelRunDepthError(f'Max model run depth hit {self.__depth}')

        if model_class is not None:
            if self.__depth == 1:
                # At top level, we use this context
                context = self
            else:
                # otherwise we create a new context

                context = EngineModelContext(chain_id=self.chain_id,
                                             block_number=self.block_number if block_number is None else block_number,
                                             web3_registry=self._web3_registry,
                                             run_id=self.run_id,
                                             depth=self.__depth,
                                             model_loader=self.__model_loader,
                                             api=api,
                                             cluster=None,
                                             model_paths=self.__model_paths
                                             )

            input = self.transform_data_for_dto(input, model_class.inputDTO, slug, 'input')

            ModelContext.current_context = context

            model = model_class(context)
            output = model.run(input)

            output = self.transform_data_for_dto(output, model_class.outputDTO, slug, 'output')

            ModelContext.current_context = self

            # If we ran with a different context, we add its deps
            if context != self:
                self._add_dependencies(context.dependencies)

            # Now we add dependency for this run
            version = model_class.version
            self._add_dependency(slug, version, 1)
        else:
            # api is not None here or get_model_class() would have
            # raised an error
            assert api is not None
            slug, version, output, dependencies = api.run_model(
                slug, version, self.chain_id,
                block_number if block_number is not None else self.block_number,
                input if input is None or isinstance(input, dict) else input.dict(),
                self.run_id, self.__depth)
            if dependencies:
                self._add_dependencies(dependencies)

        self.__depth -= 1

        return slug, version, output

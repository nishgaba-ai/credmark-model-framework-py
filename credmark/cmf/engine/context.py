import json
import logging
import traceback
import sys
from typing import Set, Type, Union
from credmark.dto.encoder import json_dumps
from credmark.cmf.model import Model
from credmark.cmf.model.context import ModelContext
from credmark.cmf.engine.errors import ModelRunRequestError
from credmark.cmf.model.errors import MaxModelRunDepthError, ModelBaseError, \
    ModelEngineError, ModelInputError, ModelNotFoundError, ModelInvalidStateError, \
    ModelOutputError, ModelRunError, ModelCallStackEntry, ModelTypeError, \
    create_instance_from_error_dict
from credmark.cmf.engine.model_api import ModelApi
from credmark.cmf.engine.model_loader import ModelLoader
from credmark.dto.transform import DataTransformError, transform_data_for_dto
from credmark.cmf.engine.web3 import Web3Registry
from credmark.dto import DTO, EmptyInput, DTOValidationError


RPC_GET_LATEST_BLOCK_NUMBER_SLUG = 'rpc.get-latest-blocknumber'


def extract_most_recent_run_model_traceback(exc_traceback, skip=1):
    # Extract "skip" most-recent group of frames before frames
    # in this file (or superclass context) until it comes back to
    # the context.
    context_files = set([__file__,
                         __file__.replace('credmark/cmf/engine/context',
                                          'credmark/cmf/model/context'),
                         __file__.replace('credmark/cmf/engine/context',
                                          'credmark/cmf/engine/model_api')])
    ptb = []
    in_other_file = False
    tb = traceback.extract_tb(exc_traceback)
    for frame in tb[::-1][:30]:  # reversed
        if frame.filename in context_files:
            if in_other_file and skip == 0:
                break
            in_other_file = False
        else:
            if not in_other_file:
                skip -= 1
            in_other_file = True
            if skip == 0:
                ptb.append(frame)
    return ''.join(traceback.format_list(ptb))


class EngineModelContext(ModelContext):
    # No doc string so it uses parent's

    # general logger:
    logger = logging.getLogger(__name__)
    # logger used for debugging models:
    debug_logger = logging.getLogger(f'{__name__}.debug')

    dev_mode = False
    max_run_depth = 20

    # Set of model slugs to use the local version.
    use_local_models_slugs: Set[str] = set()

    @classmethod
    def create_context_and_run_model(cls,  # pylint: disable=too-many-arguments,too-many-locals
                                     chain_id: int,
                                     block_number: Union[int, None],
                                     model_slug: str,
                                     model_version: Union[str, None] = None,
                                     input: Union[dict, None] = None,
                                     model_loader: Union[ModelLoader,
                                                         None] = None,
                                     chain_to_provider_url: Union[dict[str, str], None] = None,
                                     api_url: Union[str, None] = None,
                                     run_id: Union[str, None] = None,
                                     depth: int = 0):
        """
        Parameters:
            block_number: if None, latest block is used
            run_id (str | None): a string to identify a particular model run. It is
                same for any other models run from within a model.
        Catches all exceptions
        """
        context: Union[EngineModelContext, None] = None
        try:
            if model_loader is None:
                model_loader = ModelLoader(['.'])

            api = ModelApi.api_for_url(api_url)

            web3_registry = Web3Registry(chain_to_provider_url)

            if cls.dev_mode:
                cls.use_local_models_slugs.update(
                    model_loader.loaded_dev_model_slugs())
            cls.logger.debug(
                'Using local models (requested + dev models): '
                f'{cls.use_local_models_slugs}')

            if block_number is None:
                # Lookup latest block number if none specified
                block_number = cls.get_latest_block_number(api, chain_id)
                cls.logger.info(f'Using latest block number {block_number}')

            context = EngineModelContext(
                chain_id, block_number, web3_registry,
                run_id, depth, model_loader, api, True)

            if input is None:
                input = {}

            ModelContext._current_context = context

            # We set the block_number in the context above so we pass in
            # None for block_number to the run_model method.
            result_tuple = context._run_model(
                model_slug, input, None, model_version)

            output = result_tuple[2]
            output_as_dict = transform_data_for_dto(output, None, model_slug, 'output')

            response = {
                'slug': result_tuple[0],
                'version': result_tuple[1],
                'output': output_as_dict,
                'dependencies': context.dependencies}
            return response
        except ModelBaseError as err:
            response = {
                'slug': model_slug,
                'version': model_version,
                'error': err.dict(),
                'dependencies': context.dependencies if context else {}}
        except Exception as e:
            err = ModelEngineError(str(e))
            response = {
                'slug': model_slug,
                'version': model_version,
                'error': err.dict(),
                'dependencies': context.dependencies if context else {}}
        finally:
            ModelContext._current_context = None
        return response

    @classmethod
    def get_latest_block_number(cls, api: ModelApi, chain_id: int):
        _s, _v, output, _e, _d = api.run_model(RPC_GET_LATEST_BLOCK_NUMBER_SLUG,
                                               None, chain_id, 0, {}, raise_error_results=True)
        if output is None:
            raise Exception('Error response getting latest block number')

        block_number: int = output['blockNumber']
        return block_number

    def __init__(self,  # pylint: disable=too-many-arguments
                 chain_id: int,
                 block_number: int,
                 web3_registry: Web3Registry,
                 run_id: Union[str, None],
                 depth: int,
                 model_loader: ModelLoader,
                 api: Union[ModelApi, None],
                 is_top_level: bool = False):
        super().__init__(chain_id, block_number, web3_registry)
        self.run_id = run_id
        self.__depth = depth
        self.__dependencies = {}
        self.__model_loader = model_loader
        self.__api = api
        self.__is_top_level = is_top_level
        self.is_active = False

    @property
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

    def run_model(self,
                  slug,
                  input=EmptyInput(),
                  return_type=None,
                  block_number=None,
                  version=None,
                  ):
        """Run a model by slug and optional version.

        Parameters:
            slug (str): the slug of the model
            input (dict | DTO): an optional dictionary of
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
            raise ModelInvalidStateError(
                f'Attempt to run model {slug} at context block {self.block_number} '
                f'with future block {block_number}')

        res_tuple = self._run_model(slug, input, block_number, version)

        # The last item of the tuple is the output.
        output = res_tuple[-1]
        # Transform to the requested return type
        return transform_data_for_dto(output, return_type, slug, 'output')

    def _run_model(self,
                   slug: str,
                   input: Union[dict, DTO],
                   block_number: Union[int, None],
                   version: Union[str, None]
                   ):

        is_cli = self.dev_mode and not self.run_id
        is_top_level_inactive = self.__is_top_level and not self.is_active
        force_local = slug in self.use_local_models_slugs
        use_local = is_top_level_inactive or force_local
        # when using the cli, we allow running remote models as top level
        try_remote = not is_top_level_inactive or is_cli

        try:
            model_class = self.__model_loader.get_model_class(slug, version, force_local)
        except Exception:
            self.logger.error(
                f'Requested local model not found locally: slug {slug} '
                f'version {version if version is not None else "any"}')
            raise

        self.__depth += 1

        try:
            if self.__depth >= self.max_run_depth:
                raise MaxModelRunDepthError(f'Max model run depth hit {self.__depth}')

            return self._run_model_with_class(
                slug,
                input,
                block_number,
                version,
                model_class,
                use_local,
                try_remote)
        finally:
            self.__depth -= 1

    def _run_model_with_class(self,  # pylint: disable=too-many-locals,too-many-arguments
                              slug: str,
                              input: Union[dict, DTO],
                              block_number: Union[int, None],
                              version: Union[str, None],
                              model_class: Union[Type[Model], None],
                              use_local: bool,
                              try_remote: bool):

        api = self.__api

        if use_local and model_class is not None:

            slug, version, output = self._run_local_model_with_class(
                slug,
                input,
                block_number,
                version,
                model_class)

        elif try_remote and api is not None:
            try:
                debug_log = self.debug_logger.isEnabledFor(logging.DEBUG)

                if debug_log:
                    self.debug_logger.debug(f"> Run API model '{slug}' input: {input}")

                slug, version, output, error, dependencies = api.run_model(
                    slug, version, self.chain_id,
                    block_number if block_number is not None else self.block_number,
                    input if input is None or isinstance(input, dict) else input.dict(),
                    self.run_id, self.__depth)

                if dependencies:
                    self._add_dependencies(dependencies)

                # Any error raised will already have a call stack entry
                if error is not None:
                    if debug_log:
                        self.debug_logger.debug(f"< Run API model '{slug}' error: {error}")
                    raise create_instance_from_error_dict(error)

                if debug_log:
                    self.debug_logger.debug(f"< Run API model '{slug}' output: {output}")

            except ModelNotFoundError:
                # We always fallback to local if model not found on server.
                if model_class is not None:
                    self.logger.debug(f'Model {slug} not on server. Using local instead')
                    slug, version, output = self._run_local_model_with_class(
                        slug,
                        input,
                        block_number,
                        version,
                        model_class)
                else:
                    raise
        else:
            # No call stack item because model not run
            err = ModelNotFoundError.create(slug, version)
            self.logger.error(err)
            raise err

        return slug, version, output

    def _run_local_model_with_class(self,  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
                                    slug: str,
                                    input: Union[dict, DTO],
                                    block_number: Union[int, None],
                                    version: Union[str, None],
                                    model_class: Type[Model]):

        debug_log = self.debug_logger.isEnabledFor(logging.DEBUG)

        if not self.is_active:
            # At top level, we use this context
            context = self
        else:
            # otherwise we create a new context
            if block_number is None:
                block_number = self.block_number

            context = EngineModelContext(self.chain_id,
                                         block_number,
                                         self._web3_registry,
                                         self.run_id,
                                         self.__depth,
                                         self.__model_loader,
                                         self.__api
                                         )

        try:

            try:
                input = transform_data_for_dto(input, model_class.inputDTO, slug, 'input')
            except DataTransformError as err:
                # We convert to an input error here to distinguish
                # from output transform errors below
                raise ModelInputError(str(err))

            ModelContext._current_context = context
            context.is_active = True

            # Errors in this section will add the callee
            # model to the call stack

            model = model_class(context)

            if debug_log:
                self.debug_logger.debug(f"> Run model '{slug}' input: {input}")

            output = model.run(input)

            try:
                # transform to the defined outputDTO for validation of output
                output = transform_data_for_dto(output, model_class.outputDTO, slug, 'output')
                if self.dev_mode:
                    # In dev mode we do a deep transform to dicts (convert all DTOs)
                    # We do this to ensure dev is same as production.
                    # Production mode will serialize all input and output.
                    output = json.loads(json_dumps(output))
            except DataTransformError as err:
                raise ModelOutputError(str(err))

            if debug_log:
                self.debug_logger.debug(f"< Run model '{slug}' output: {output}")

        except Exception as err:
            if isinstance(err, (DataTransformError, DTOValidationError)):
                # Transform error is a coding error in model just run
                err = ModelTypeError(str(err))
                trace = traceback.format_exc(limit=30)
                if debug_log:
                    self.debug_logger.debug(f"< Run model '{slug}' error: {err}")

            elif isinstance(err, ModelBaseError):
                _exc_type, _exc_value, exc_traceback = sys.exc_info()
                if isinstance(err, (ModelNotFoundError, ModelRunRequestError)):
                    trace = extract_most_recent_run_model_traceback(exc_traceback, 2)
                else:
                    trace = extract_most_recent_run_model_traceback(exc_traceback)

                # For errors that have specific detail classes, we
                # ensure detail is a dict (as it will be over the wire)
                # to make local-dev identical to production.
                err.transform_data_detail(None)

                if debug_log:
                    self.debug_logger.debug(f"< Run model '{slug}' error: {err}")
            else:
                err_msg = f'Exception running model {slug}({input}) on ' \
                    f'chain {context.chain_id} ' \
                    f'block {context.block_number} (' \
                    f'{context.block_number.timestamp_datetime:%Y-%m-%d %H:%M:%S}) ' \
                    f'with {err}'
                if self.dev_mode:
                    self.logger.exception(err_msg)
                elif debug_log:
                    self.debug_logger.debug(f"< Run model '{slug}' error: {err_msg}")

                err = ModelRunError(err_msg)
                trace = traceback.format_exc(limit=30)

            # We add the model just run (or validated input for) to stack
            err.data.stack.insert(0,
                                  ModelCallStackEntry(
                                      slug=slug,
                                      version=model_class.version,
                                      chainId=context.chain_id,
                                      blockNumber=context.block_number,
                                      trace=trace if trace is not None else None))
            raise err

        finally:
            context.is_active = False
            ModelContext._current_context = self

            # If we ran with a different context, we add its deps
            if context != self:
                self._add_dependencies(context.dependencies)

            # Now we add dependency for this run
            version = model_class.version
            self._add_dependency(slug, version, 1)

        return slug, version, output

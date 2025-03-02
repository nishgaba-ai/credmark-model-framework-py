# Model Framework Core Components

In the following you will find the key components of every model.

## Model Class

Credmark uses a simple base class called ‘Model’ class to set up a model. The actual code can be found [here](https://github.com/credmark/credmark-model-framework-py/blob/main/credmark/cmf/model/__init__.py).

All Models should import this class `import credmark.cmf.model` and can override the run() method. See examples [here](https://github.com/credmark/credmark-models-py/tree/main/models/examples).

The `@Model.describe()` decorator provides a simple interface to define the model properties such as slug, version, display_name, description, developer, input, output etc so that it can be used easily by consumers and other models.

If description is not specified, the `__doc__` string of the model's class is used for the model description.

See example [here](https://github.com/credmark/credmark-models-py/blob/main/models/examples/address_examples.py).

## Data Transfer Object (DTO)

Input and output data for models are json-serializable objects of arbitrary depth and complexity. Objects can have 0 or more keys whose values can be null, number, string, array, or another object.

Although you can use dictionaries for model input and output data in your python code, we strongly encourage the use of DTOs (Data Transfer Objects.)

DTOs are classes with typed properties which will serialize and deserialize to and from JSON. They also automatically produce a JSON-schema that is used to document the input and output of a model. Each model may have their own DTOs or may share or inherit a DTO from another model that you have developed.

To create a DTO, simply subclass the DTO base class and use DTOFields to annotate your properties. Under the hood, the Credmark Model Framework uses the pydantic python module (DTO is simply an alias for pydantic BaseModel and DTOField an alias for Field) so almost anything that works with pydantic will work for your DTO.

Please see the [pydantic docs](https://pydantic-docs.helpmanual.io/usage/models/) for more information.

### Model Error Detail DTO

Besides input and output, subclases of `ModelBaseError` can use a DTO for the `data.detail` object instead of a dict. You can simply pass a DTO as the `detail` arg in a model constructor:

```python
address = Address(some_address_string)
e = ModelDataError(message='Address is not a contract',
                   code=ModelDataError.Codes.CONFLICT,
                   detail=address)
```

If your detail object has many properties and you want to document the error and details, you can create a custom DTO and error class:

- Create a DTO subclass that defines the data you want to store in the detail.

For example:

```python
class TokenAddressNotFoundDetailDTO(DTO):
    address: Address = DTOField(...,description='Address for token not found')
```

- Create a DTO subclass that defines the new error DTO. (This step is not strictly necessary but it lets you document the error.) The trick is to use the generic properties of the `ModelErrorDTO` to specify the detail's DTO class: `ModelErrorDTO[TokenAddressNotFoundDetailDTO]`

```python
class TokenAddressNotFoundDTO(ModelErrorDTO[TokenAddressNotFoundDetailDTO]):
  """
  This error occurs when there is no token at the specified address.
  The detail contains the address.
  """
```

- Then create a `ModelDataError` (or `ModelRunError`) subclass and set the class property `dto_class` to your new error DTO class:

```python
class TokenAddressNotFoundError(ModelDataError):
    dto_class = TokenAddressNotFoundDTO
```

- You can now create an error instance with:

```python
# bad_address is set to an Address instance
error = TokenAddressNotFoundError(message='Bad address',
                                detail=TokenAddressNotFoundDetailDTO(address=bad_address))
# You can now access: error.data.detail.address
```

## Model Types
We also have some built-in reusable type classes available under [Credmark.cmf.types](https://github.com/credmark/credmark-model-framework-py/tree/main/credmark/cmf/types).

We have created and grouped together different classes to manage input and output types to be used by models. These types include some standard blockchain and financial data structures as well as some standard input and output objects for Credmark models.

### models

1. ledger.py : DTOs and data used by the ledger models
2. series.py: DTOs for the series models

### data

**1. Address:** this class is a subclass of string and holds a blockchain address.

`Address` class is inherited from `str` to help with web3 address conversion. It's highly recommended to use it instead of a baremetal address.

✔️: Address("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9").checksum # checksum version to be used

❌: Address("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9") # lower case version

❌:"0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9" # lower case version

Example:

        from credmark.cmf.types import (Address, Contract)

        contract = Contract(
            # lending pool address
            address=Address("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9").checksum,
            abi=AAVE_V2_TOKEN_CONTRACT_ABI
        )

The address can be provided in lower case, upper case or checksum hex format. This class will normalize the address into lower case. Note that It can be used as a normal string but it also has a "checksum" property which returns a web3 ChecksumAddress.

See [address_examples.py](https://github.com/credmark/credmark-models-py/blob/main/models/examples/address_examples.py) on how to use this class.

**2. Account(s):** Account simply holds an address. Accounts is a list of account instances which allows iteration through each account.

See [iteration_example](https://github.com/credmark/credmark-models-py/blob/main/models/examples/iteration_examples.py) on how to use this class.

**3. Contract:** a contract is a subclass of Account which has a name, deployed transaction hash, abi, protocol name etc..

Object instantiation of this class will load all information available for the contract (against contract address provided as input) in our database and you can access whatever information you want from the object.

See [contact_example.py](https://github.com/credmark/credmark-models-py/blob/main/models/examples/contract_examples.py) on how to use this class.

**4. Token:** Token is a specific kind of contract; hence the Token class inherits from Contract class.

This class allows you to load token information with an address or symbol as well as get its price in USD Currently this class supports data load for erc20 token but we will support erc721 as well soon.

See [token_example.py](https://github.com/credmark/credmark-models-py/blob/main/models/examples/token_examples.py) on how to use this class. Token_data.py lists all erc20 tokens currently supported.

**5. Price and TokenPairPrice:** these classes can be used to hold a price or a price of a token against a reference token (such as CMK-BTC, CMK-ETH etc.)

**6. Position:** this class holds a Token and an amount It can calculate its value based on the token price in USD. You can also access the scaled amount property if you need the scaled amount of the erc20 token.

Token_data.py lists all erc20 tokens currently supported.

**7. Portfolio:** This class holds a list of positions. So, it can be used to calculate all positions within a wallet.




## Model Context

Each model runs with a particular context, including the name of the blockchain, block number, and a configured web3 instance (among other things). The context can be passed along when the model calls other models. The context’s web3 instance can be used to make RPC calls.
The `ModelContext()` Class sets up the context for the model to run and can be accessed from a model as `self.context`.
The base code can be found [here](https://github.com/credmark/credmark-model-framework-py/blob/main/credmark/cmf/model/context.py). It provides an interface for models to run other models, call contracts, get ledger data, use a web3 instance etc.

It also enforces deterministic behavior for Models. The key utilities in `ModelContext` are

- web3
- contract
- ledger
- block number
- historical utility

### Calling Other Models

A model can call other models and use their results. You can pass the input as an input arg and the model output is returned as a dict (or DTO if `return_type` is specified.)

If an error occurs during a call to run a model, an exception is raised. See [Error handling](#error-handling)

There are 2 ways to call another model:

- Using `context.models` (Recommended)
- Calling `context.run_model()`

#### `context.models`

Models are exposed on `context.models` by their slug (with any "-" (hyphens) in the slug replaced with "\_" (underscores)) and can be called like a function, passing the input as a DTO or dict or as standard keyword args (kwargs).

For example, here we use keyword args:

```python
# Returns a dict with output of the model
result = self.context.models.example.echo(message='Hello world')
```

You can use a DTO for the output by inializing it with the output dict.

Here we use a DTO instance as the input and convert the output to a DTO instance (in this case they happen to be the same DTO class but they don't have to be):

```python
class EchoDto(DTO):
    message: str = DTOField('Hello', description='A message')

input = EchoDto(message='Hello world')
echo = EchoDto(**self.context.models.example.echo(input))

echo.message # will equal 'Hello world'
```

You can run a model at a different block number by using the `context.models(block_number=12345)` syntax, for example:

```python
# Runs the model with a context of block number 12345
result = self.context.models(block_number=12345).example.echo(message='Hello world')
```

#### `context.run_model()`

Alternatively you can run a model by slug string using the `context.run_model` method:

```python
def run_model(name: str,
          input: Union[dict, DTO] = EmptyInput(),
          return_type: Union[Type[dict], Type[DTO], None],
          block_number: Union[int, None] = None,
          version: Union[str, None] = None)
```

If `return_type` is None or dict, then the method returns the model output as a dict. If it's a DTO class, the method returns a DTO instance. As above, you can use a dict result with `**` to initialize a DTO instance yourself.

For example:

```python
# token = Token( ) instance

price = Price(**self.context.run_model('price', token))

# has the same effect as:

price = self.context.run_model('price', token, return_type=credmark.cmf.types.Price)
```

### Web3

`context.web3` will return a configured web3 instance with the default block set to the block number of context.
The web3 providers are determined from the environment variables as described in the configuration section above. Currently users will need to use their own alchemy account (or other web3 provider) to access web3 functionality.

### Contract

Credmark simplified the process of getting web3 instances of any contract from any chain. So you don't need to find and hardcode chain specific attributes and functions within these chains to run your models.

The model context exposes the `context.contracts` property which can be used to get contracts by metadata or address. The contracts are instances of the `Contract` class which are configured and use the web3 instance at specified block number and specified chain id along with additional data based on `constructor_args`.

Example code for contact class can be found [here](https://github.com/credmark/credmark-model-framework-py/blob/main/credmark/cmf/types/contract.py).

Currently below parameters as argument are supported to fetched using Contracts:

- name: name of the contract
- address: address of the contract
- deploy_tx_hash: transaction hash at which contract was deployed
- Constructor_args
- protocol: protocol name
- product: product name
- abi_hash
- abi

Contract functions are accessible using the `contract.functions` property.

Tip: the contract object returned from contract class can be used to fetch any specific web3 attributes of the contract and call contract functions. As well it can be used as a DTO (see details below) so it can be returned as part of the output of a model.

### Ledger

Credmark allows access to in-house blockchain ledger data via ledger interface (`context.ledger`), so that any model can fetch/use ledger data if required. This is done via `Ledger` class which currently supports below functions:

- get_transactions
- get_traces
- get_logs
- get_contracts
- get_blocks
- get_receipts
- get_erc20_tokens
- get_erc20_transfers

Please refer [here](https://github.com/credmark/credmark-model-framework-py/blob/main/credmark/cmf/model/ledger/__init__.py) for the code of the `Ledger` class.

### Block number

The `context.block_number` holds the block number for which a model is running. Models only have access to data at (by default) or before this block number (by instantiating a new context). In other words models cannot see into the future and ledger queries etc. will restrict access to data by this block number.
As a subclass of int, the `block_number` class allows the provided block numbers to be treated as integers and hence enables arithmetic operations on block numbers. It also allows you to fetch the corresponding datetime and timestamp properties for the block number. This can be super useful in case we want to run any model iteratively for a certain block-interval or time-interval backwards from the block number provided in the context.

Example code for the block-number class can be found [here](https://github.com/credmark/credmark-model-framework-py/blob/main/credmark/cmf/types/block_number.py).

__Block number, Timestamp and Python datetime__

In blockchain, every block is created with a timestamp (in Unix epoch). In Python there are two types for date, date and datetime, with datetime can be with tzinfo or without. To provide convienent tools to query between the three and resolve the confusion around time, we have a few tools with `BlockNumber` class.

1. property, `block_number.timestamp_datetime`: Return the Python datetime with UTC of the block.

2. property, `block_number.timestamp`: Return the Unix epoch of the block.

3. class method: `from_datetime(cls, timestamp: int)`: Return a BlockNumber instance to be less or equal to the input timestamp.

    Be cautious when we obtain a timestamp from a Python datetime, we should attach a tzinfo (e.g. timezone.utc) to the datetime. Otherwise, Python take account of the local timezone when converting to a timestamp. See the mode  [`example.block-time`](https://github.com/credmark/credmark-models-py/blob/main/models/examples/block_time_example.py).

4. Use a BlockNumber instance: Obtain a Python datetime with UTC of the block. The block number should be less or equal to the context block.

    ```
    from credmark.types import ( BlockNumber )

    dt = BlockNumber(14234904).timestamp_datetime
    ```

More example code for the block-number class can be found in [here](https://github.com/credmark/credmark-model-framework-py/blob/main/credmark/types/data/block_number.py) and model [`example.block-time`](https://github.com/credmark/credmark-models-py/blob/main/models/examples/block_time_example.py).


### Historical Utility

The historical utility, available at `context.historical` (see [here](https://github.com/credmark/credmark-model-framework-py/blob/main/credmark/cmf/model/utils/historical_util.py)), allows you to run a model over a series of blocks for any defined range and interval.

Block ranges can be specified by blocks (either a window from current block or a start and end block) or by time (a window from the current block’s time or start and end time.) Times can be specified different units, i.e. year, month, week, day, hour, minute and second.

See [historical_example.py](https://github.com/credmark/credmark-models-py/blob/main/models/examples/historical_examples.py) on how to use this class.


import credmark.cmf.model
from credmark.cmf.model.errors import ModelDataError, ModelRunError

from .contract import Contract
from .address import Address
from .data.fungible_token_data import FUNGIBLE_TOKEN_DATA, ERC20_GENERIC_ABI
from typing import List, Union
from credmark.dto import PrivateAttr, IterableListGenericDTO, DTOField, DTO  # type: ignore
from web3.exceptions import (
    BadFunctionCallOutput,
    ABIFunctionNotFound
)


def get_token_from_configuration(
        chain_id: str,
        symbol: Union[str, None] = None,
        address: Union[Address, None] = None,
        is_native_token: bool = False,
        wraps: Union[str, None] = None) -> Union[dict, None]:
    token_datas = [
        t for t in FUNGIBLE_TOKEN_DATA.get(chain_id, [])
        if (t.get('is_native_token', False) == is_native_token and
            (t.get('address', None) == address or
             t.get('symbol', None) == symbol) or
            (t.get('wraps', None) == wraps and wraps is not None))
    ]

    if len(token_datas) > 1:
        # TODO: Until we have definitive token lookup, we'll
        # consider it transient as a ModelRunError.
        raise ModelRunError(
            'Missing fungible token data in lookup for '
            f'chain_id={chain_id} symbol={symbol} '
            f'address={address} is_native_token={is_native_token}')

    if len(token_datas) == 1:
        return token_datas[0]

    return None


class Token(Contract):
    """
    Token represents a fungible Token that conforms to ERC20
    standards
    """

    class TokenMetadata(Contract.ContractMetaData):
        symbol: Union[str, None] = None
        name: Union[str, None] = None
        decimals: Union[int, None] = None
        total_supply: Union[int, None] = None

    _meta: TokenMetadata = PrivateAttr(
        default_factory=lambda: Token.TokenMetadata())  # pylint: disable=unnecessary-lambda

    class Config:
        schema_extra = {
            'examples': [{'address': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'},
                         {'symbol': 'AAVE'}
                         ] + Contract.Config.schema_extra['examples']
        }

    def __init__(self, **data):

        if 'address' not in data and 'symbol' in data:
            context = credmark.cmf.model.ModelContext.current_context()

            token_data = get_token_from_configuration(
                chain_id=str(context.chain_id), symbol=data['symbol'])
            if token_data is not None:
                data['address'] = token_data['address']
                if 'meta' not in data:
                    data['meta'] = {}
                data['meta']['symbol'] = token_data['symbol']
                data['meta']['name'] = token_data['name']
                data['meta']['decimals'] = token_data['decimals']

        if data['address'] == Address.null():
            raise ModelDataError(f'NULL address ({Address.null()}) is not a valid Token Address')

        super().__init__(**data)

    def _load(self):
        if self._loaded:
            return

        if self._meta.abi is None:
            self._meta.abi = ERC20_GENERIC_ABI

        super()._load()

    @property
    def info(self):
        if isinstance(self, TokenInfo):
            return self
        self._load()

        return TokenInfo(**self.dict(), meta=self._meta)

    def try_erc20_property(self, prop_name):
        try:
            prop = self.functions[prop_name]().call()  # type: ignore
        except (BadFunctionCallOutput, ABIFunctionNotFound):
            raise ModelDataError(
                f'No {prop_name} function on token {self.address}, non ERC20 Compliant'
                f' proxied by {self.proxy_for.address}' if self.proxy_for is not None else '')
        if prop is None:
            raise ModelDataError(f"Token.{prop_name} is None")
        return prop

    @property
    def symbol(self):
        self._load()
        if self._meta.symbol is None:
            self._meta.symbol = self.try_erc20_property('symbol')
        return self._meta.symbol

    @property
    def decimals(self) -> int:
        self._load()
        if self._meta.decimals is None:
            self._meta.decimals = self.try_erc20_property('decimals')
        return self._meta.decimals

    @property
    def name(self):
        self._load()
        if self._meta.name is None:
            self._meta.name = self.try_erc20_property('name')
        return self._meta.name

    @property
    def total_supply(self):
        self._load()
        if self._meta.total_supply is None:
            self._meta.total_supply = self.try_erc20_property('totalSupply')
        return self._meta.total_supply

    def scaled(self, value):
        return value / (10 ** self.decimals)


class TokenInfo(Token):
    meta: Token.TokenMetadata


class NativeToken(DTO):
    symbol: str = 'ETH'
    name: str = 'ethereum'
    decimals: int = 18

    def __init__(self, **data) -> None:
        context = credmark.cmf.model.ModelContext.current_context()

        token_data = get_token_from_configuration(
            chain_id=str(context.chain_id), is_native_token=True)
        if token_data is not None:
            data['symbol'] = token_data.get('symbol')
            data['decimals'] = token_data.get('decimals')
            data['name'] = token_data.get('name')
        super().__init__(**data)

    def scaled(self, value):
        return value / (10 ** self.decimals)

    def wrapped(self) -> Union[Token, None]:
        context = credmark.cmf.model.ModelContext.current_context()

        token_data = get_token_from_configuration(
            chain_id=str(context.chain_id), wraps=self.symbol)
        if token_data is not None:
            return Token(address=token_data['address'])
        return None


class NonFungibleToken(Contract):
    def __init__(self, **data):
        super().__init__(**data)
        raise NotImplementedError()


class Tokens(IterableListGenericDTO[Token]):
    tokens: List[Token] = DTOField(
        default=[], description="An iterable list of Token Objects")
    _iterator: str = PrivateAttr('tokens')

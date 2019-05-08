try:
    from . import _binary_decoder
    from ._binary_decoder import ReadError
except ImportError:
    from . import _binary_decoder_py as _binary_decoder

    class ReadError(Exception):
        pass


BinaryDecoder = _binary_decoder.BinaryDecoder

__all__ = ['BinaryDecoder']

import base64
import hashlib
import hmac
import json


class CursorError(ValueError):
    pass


class Cursors:
    def __init__(self, key):
        if len(key) < 32:
            raise ValueError('Cursor signing key must contain at least 32 bytes')
        self.key = key

    def encode(self, context, position):
        body = json.dumps(dict(schema=1, context=context, position=position), sort_keys=True, separators=(',', ':')).encode()
        signature = hmac.new(self.key, body, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(signature+body).decode().rstrip('=')

    def decode(self, token, context):
        try:
            if len(token) > 4096:
                raise ValueError()
            raw = base64.b64decode(token+'='*(-len(token)%4), altchars=b'-_', validate=True)
            signature, body = raw[:32], raw[32:]
            if not hmac.compare_digest(signature, hmac.new(self.key, body, hashlib.sha256).digest()):
                raise ValueError()
            result = json.loads(body)
            if result['schema'] != 1 or result['context'] != context:
                raise ValueError()
            return result['position']
        except (ValueError, KeyError, TypeError, UnicodeError) as error:
            raise CursorError('Invalid cursor for this release and filter') from error

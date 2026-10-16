from collections import namedtuple

from .utilities import class_path, constantize

Definition = namedtuple("Definition", "type arguments")


class InterceptorRegistry:
    """Ordered ``around(context, proceed)`` chain. ``proceed()`` continues downstream."""

    def __init__(self, definitions=()):
        self._definitions = tuple(Definition(d.type, tuple(d.arguments)) for d in definitions)

    def __iter__(self):
        return iter(self._definitions)

    def __len__(self):
        return len(self._definitions)

    def empty(self):
        return not self._definitions

    def use(self, interceptor_type, *arguments):
        self.discard(interceptor_type)
        self._definitions += (Definition(interceptor_type, tuple(arguments)),)
        return self

    def discard(self, interceptor_type):
        self._definitions = tuple(d for d in self._definitions if d.type is not interceptor_type)
        return self

    def call(self, context, operation):
        continuation = operation
        for definition in reversed(self._definitions):
            interceptor = definition.type(*definition.arguments)
            continuation = self._link(interceptor, context, continuation)
        return continuation()

    @staticmethod
    def _link(interceptor, context, downstream):
        return lambda: interceptor.around(context, downstream)

    def export(self):
        return [[class_path(d.type), list(d.arguments)] for d in self._definitions]

    def import_(self, records):
        self._definitions = tuple(Definition(constantize(path), tuple(args)) for path, args in records)
        return self

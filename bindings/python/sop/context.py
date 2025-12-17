from . import call_go


class Context:
    """
    Represents a context for SOP operations.

    The context is used to manage the lifecycle of operations, handle errors, and allow for cancellation.
    It serves as a bridge between the Python client and the underlying Go implementation.
    """

    def __init__(self):
        """
        Initializes a new Context.

        This creates a corresponding context in the Go runtime and stores its ID.
        """
        self._removed = False
        self.id = call_go.create_context()

    def __del__(self):
        """
        Clean up the context.

        Removes the corresponding context from the Go runtime.
        """
        if not self._removed:
            self._removed = True
            call_go.remove_context(self.id)

    def cancel(self):
        """
        Cancels any running operation associated with this context.

        This signals the underlying Go runtime to abort the operation.
        """
        call_go.cancel_context(self.id)
        self._removed = True

    def error(self) -> str:
        """
        Checks for errors in the context.

        Returns:
            str: The error message if an error occurred (e.g., timeout or canceled), otherwise None.
        """
        return call_go.context_error(self.id)

    def is_valid(self) -> bool:
        """
        Checks if the context is valid.

        Returns:
            bool: True if the context has no errors, False otherwise.
        """
        return self.error() is None

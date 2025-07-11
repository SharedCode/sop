from . import call_go


class Context:
    """
    Use context as parameter when calling SOP Transaction & Btree methods. And if your code needs to, use
    it to cancel any ongoing method, e.g. transaction commit. While your code is running on another thread and commit
    is happening, your code can call the context's cancel method to tell the commit to abort.

    That is what a context is for coming from the Python side, option for execution abortion if/when necessary.
    """

    def __init__(self):
        """
        Constructor auto creates a constructor on the other side and keep the 'id' so it can get "managed" later on.
        """
        self._removed = False
        self.id = call_go.create_context()

    def __del__(self):
        """
        Destructor auto removes the context from the other side.
        """
        if not self._removed:
            self._removed = True
            call_go.remove_context(self.id)

    def cancel(self):
        """
        cancel or abort any running function within a context.
        """
        call_go.cancel_context(self.id)
        self._removed = True

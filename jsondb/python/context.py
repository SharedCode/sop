import call_go


class Context:
    def __init__(self):
        self._removed = False
        self.id = call_go.create_context()

    def __del__(self):
        if not self._removed:
            self._removed = True
            call_go.remove_context(self.id)

    def cancel(self):
        call_go.cancel_context(self.id)
        self._removed = True

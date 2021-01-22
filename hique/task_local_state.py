import asyncio
from asyncio.tasks import Task
from typing import Any, Callable, Dict, Generic, TypeVar

T = TypeVar("T")


class TaskLocalState(Generic[T]):
    _states: Dict[int, T]

    def __init__(self, default: Callable[[], T]):
        self._default = default
        self._states = {}

    @property
    def current(self) -> T:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("No current task.")

        task_id = id(task)
        state = self._states.get(task_id)
        if state is None:
            state = self._states[task_id] = self._default()
            task.add_done_callback(self._delete_state)
        return state

    def _delete_state(self, task: Task[Any]) -> None:
        del self._states[id(task)]

"""The Govee learned storage yaml file manager."""

from dataclasses import asdict
import logging
import dacite
import yaml

from homeassistant.util.yaml import load_yaml, save_yaml
from .models import GoveeLearnedInfo

_LOGGER = logging.getLogger(__name__)
LEARNING_STORAGE_YAML = "/govee_learning.yaml"


class GoveeLearningStorage:
    def __init__(self, config_dir, hass):
        self._config_dir = config_dir
        self._hass = hass

    async def read(self):
        path = self._config_dir + LEARNING_STORAGE_YAML

        def _blocking_read():
            return load_yaml(path)

        learned_info = {}
        try:
            learned_dict = await self._hass.async_add_executor_job(_blocking_read)
            learned_info = {
                dev: dacite.from_dict(GoveeLearnedInfo, learned_dict[dev])
                for dev in learned_dict
            }
        except FileNotFoundError:
            _LOGGER.info("No learned info file yet, starting fresh")
        except Exception as ex:
            _LOGGER.warning("Invalid learned info file %s: %s", path, ex)

        return learned_info

    async def write(self, learned_info):
        path = self._config_dir + LEARNING_STORAGE_YAML
        learned_dict = {dev: asdict(learned_info[dev]) for dev in learned_info}

        def _blocking_write():
            save_yaml(path, learned_dict)

        await self._hass.async_add_executor_job(_blocking_write)
        _LOGGER.info("Stored learning information to %s", path)

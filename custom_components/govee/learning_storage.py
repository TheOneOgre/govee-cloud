"""The Govee learned storage yaml file manager."""

import asyncio
from dataclasses import asdict
import logging

import dacite
from govee_api_laggat import GoveeAbstractLearningStorage, GoveeLearnedInfo
import yaml

from homeassistant.util.yaml import load_yaml, save_yaml

_LOGGER = logging.getLogger(__name__)
LEARNING_STORAGE_YAML = "/govee_learning.yaml"


class GoveeLearningStorage(GoveeAbstractLearningStorage):
    """The govee_api_laggat library uses this to store learned information about lights."""

    def __init__(self, config_dir, hass, *args, **kwargs):
        """Get the config directory and Home Assistant instance."""
        super().__init__(*args, **kwargs)
        self._config_dir = config_dir
        self._hass = hass  # store hass so we can offload blocking I/O

    async def read(self):
        """Restore from yaml file (offloaded to executor)."""
        path = self._config_dir + LEARNING_STORAGE_YAML

        def _blocking_read():
            try:
                return load_yaml(path)
            except FileNotFoundError:
                raise
            except Exception as ex:
                raise ex

        learned_info = {}
        try:
            learned_dict = await self._hass.async_add_executor_job(_blocking_read)
            learned_info = {
                device: dacite.from_dict(
                    data_class=GoveeLearnedInfo, data=learned_dict[device]
                )
                for device in learned_dict
            }
            _LOGGER.info("Loaded learning information from %s.", path)
        except FileNotFoundError:
            _LOGGER.warning(
                "There is no %s file containing learned information about your devices. "
                "This is normal for first start of Govee integration.",
                path,
            )
        except (
            dacite.DaciteError,
            TypeError,
            UnicodeDecodeError,
            yaml.YAMLError,
        ) as ex:
            _LOGGER.warning(
                "The %s file containing learned information about your devices is invalid: %s. "
                "Learning starts from scratch.",
                path,
                ex,
            )
        return learned_info

    async def write(self, learned_info):
        """Save to yaml file (offloaded to executor)."""
        path = self._config_dir + LEARNING_STORAGE_YAML
        leaned_dict = {device: asdict(learned_info[device]) for device in learned_info}

        def _blocking_write():
            save_yaml(path, leaned_dict)

        await self._hass.async_add_executor_job(_blocking_write)
        _LOGGER.info("Stored learning information to %s.", path)

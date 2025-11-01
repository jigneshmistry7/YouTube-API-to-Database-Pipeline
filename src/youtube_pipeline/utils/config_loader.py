import yaml
import json
import os
from pathlib import Path
from typing import Dict, Any

class ConfigLoader:
    """Load and manage configuration files"""
    
    @staticmethod
    def load_yaml_config(file_path: str) -> Dict[str, Any]:
        """Load YAML configuration file"""
        try:
            with open(file_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {file_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML config: {e}")
    
    @staticmethod
    def load_json_config(file_path: str) -> Dict[str, Any]:
        """Load JSON configuration file"""
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON config: {e}")
    
    @staticmethod
    def load_environment_variables(prefix: str = "YOUTUBE_") -> Dict[str, Any]:
        """Load environment variables with given prefix"""
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Remove prefix and convert to lowercase
                config_key = key[len(prefix):].lower()
                config[config_key] = value
        
        return config
    
    @staticmethod
    def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple configuration dictionaries"""
        merged = {}
        
        for config in configs:
            for key, value in config.items():
                if isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
                    # Recursively merge nested dictionaries
                    merged[key] = ConfigLoader.merge_configs(merged[key], value)
                else:
                    merged[key] = value
        
        return merged
#!/usr/bin/env python3
import os
import sys
import unittest
import tempfile
import shutil
from unittest.mock import patch, MagicMock

# Add parent directory to path so we can import the script
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import gen_cfg_schema

class TestGenCfgSchema(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.output_file = os.path.join(self.temp_dir, 'cfg_schema.h')
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    @patch('sonic_yang.SonicYang')
    def test_main_with_valid_input(self, mock_sonic_yang_class):
        """Test the main function with valid input"""
        # Mock sonic_yang behavior
        mock_sy = MagicMock()
        mock_sonic_yang_class.return_value = mock_sy
        mock_sy.confDbYangMap = {
            'VLAN': {'container': {}},
            'PORT': {'container': {}},
            'memory-usage': {'container': {}},
            'TEAMD': {'container': {}}
        }
        
        # Run the script with test arguments
        with patch('sys.argv', ['gen_cfg_schema.py', '-o', self.output_file]):
            gen_cfg_schema.main()
        
        # Check if the file exists
        self.assertTrue(os.path.exists(self.output_file))
        
        with open(self.output_file, 'r') as f:
            content = f.read()
            
            # Check for expected defines
            self.assertIn('#define CFG_VLAN_TABLE_NAME "VLAN"', content)
            self.assertIn('#define CFG_PORT_TABLE_NAME "PORT"', content)
            self.assertIn('#define CFG_TEAMD_TABLE_NAME "TEAMD"', content)
            self.assertIn(
                '#define CFG_TEAMD_MODE_TABLE_NAME CFG_TEAMD_TABLE_NAME',
                content)
            # Check that memory-usage is commented out due to hyphen
            self.assertIn('// #define CFG_memory-usage_TABLE_NAME "memory-usage"', content)

        
if __name__ == '__main__':
    unittest.main() 

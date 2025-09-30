# Databricks notebook source
import re
import os
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class SQLObjectType(Enum):
    """Enumeration of SQL object types."""
    TABLE = "tables"
    VIEW = "views"
    FUNCTION = "functions"
    PROCEDURE = "procedures"
    SEQUENCE = "sequences"
    STAGE = "stages"
    PIPE = "pipes"
    TASK = "tasks"
    WAREHOUSE = "warehouses"
    DATABASE = "databases"
    SCHEMA = "schemas"
    INSERT = "inserts"
    OTHER = "others"


@dataclass
class SQLObject:
    """Represents a parsed SQL object."""
    name: str
    object_type: SQLObjectType
    sql_content: str
    start_line: int
    end_line: int
    file_path: str
    dependencies: List[str] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []


class SnowflakeSQLParser:
    """Robust SQL parser for Snowflake SQL files that handles complex structures."""
    
    def __init__(self):
        # Patterns for different SQL object types
        self.patterns = {
            SQLObjectType.TABLE: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+',
                r'ALTER\s+TABLE\s+',
                r'DROP\s+TABLE\s+'
            ],
            SQLObjectType.VIEW: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+',
                r'ALTER\s+VIEW\s+',
                r'DROP\s+VIEW\s+'
            ],
            SQLObjectType.FUNCTION: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+',
                r'ALTER\s+FUNCTION\s+',
                r'DROP\s+FUNCTION\s+'
            ],
            SQLObjectType.PROCEDURE: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+',
                r'ALTER\s+PROCEDURE\s+',
                r'DROP\s+PROCEDURE\s+'
            ],
            SQLObjectType.SEQUENCE: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?SEQUENCE\s+',
                r'ALTER\s+SEQUENCE\s+',
                r'DROP\s+SEQUENCE\s+'
            ],
            SQLObjectType.STAGE: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?STAGE\s+',
                r'ALTER\s+STAGE\s+',
                r'DROP\s+STAGE\s+'
            ],
            SQLObjectType.PIPE: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?PIPE\s+',
                r'ALTER\s+PIPE\s+',
                r'DROP\s+PIPE\s+'
            ],
            SQLObjectType.TASK: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+',
                r'ALTER\s+TASK\s+',
                r'DROP\s+TASK\s+'
            ],
            SQLObjectType.WAREHOUSE: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?WAREHOUSE\s+',
                r'ALTER\s+WAREHOUSE\s+',
                r'DROP\s+WAREHOUSE\s+'
            ],
            SQLObjectType.DATABASE: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?DATABASE\s+',
                r'ALTER\s+DATABASE\s+',
                r'DROP\s+DATABASE\s+'
            ],
            SQLObjectType.SCHEMA: [
                r'CREATE\s+(?:OR\s+REPLACE\s+)?SCHEMA\s+',
                r'ALTER\s+SCHEMA\s+',
                r'DROP\s+SCHEMA\s+'
            ],
            SQLObjectType.INSERT: [
                r'INSERT\s+INTO\s+'
            ]
        }
        
        # Compile patterns for efficiency
        self.compiled_patterns = {}
        for obj_type, patterns in self.patterns.items():
            self.compiled_patterns[obj_type] = [re.compile(pattern, re.IGNORECASE | re.MULTILINE) for pattern in patterns]

    def parse_file(self, file_path: str) -> List[SQLObject]:
        """
        Parse a SQL file and extract individual SQL objects.
        
        Args:
            file_path: Path to the SQL file
            
        Returns:
            List of SQLObject instances
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            return self.parse_content(content, file_path)
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            return []

    def parse_content(self, content: str, file_path: str = "") -> List[SQLObject]:
        """
        Parse SQL content and extract individual SQL objects.
        
        Args:
            content: SQL content as string
            file_path: Optional file path for context
            
        Returns:
            List of SQLObject instances
        """
        lines = content.split('\n')
        objects = []
        
        # Find all potential SQL objects
        for obj_type, patterns in self.compiled_patterns.items():
            for pattern in patterns:
                for match in pattern.finditer(content):
                    start_pos = match.start()
                    start_line = content[:start_pos].count('\n') + 1
                    
                    # Extract the complete SQL object
                    sql_obj = self._extract_complete_object(
                        content, start_pos, obj_type, lines, start_line, file_path
                    )
                    
                    if sql_obj:
                        objects.append(sql_obj)
        
        # Sort objects by start line to maintain order
        objects.sort(key=lambda x: x.start_line)
        
        # Remove overlapping objects (keep the first one)
        filtered_objects = self._remove_overlapping_objects(objects)
        
        return filtered_objects

    def _extract_complete_object(self, content: str, start_pos: int, obj_type: SQLObjectType, 
                               lines: List[str], start_line: int, file_path: str) -> Optional[SQLObject]:
        """Extract a complete SQL object starting from the given position."""
        
        # Find the end of the SQL object
        end_pos = self._find_object_end(content, start_pos, obj_type)
        
        if end_pos == -1:
            return None
        
        # Extract SQL content
        sql_content = content[start_pos:end_pos].strip()
        
        # Extract object name
        object_name = self._extract_object_name(sql_content, obj_type)
        
        # Calculate end line
        end_line = content[:end_pos].count('\n') + 1
        
        # Extract dependencies
        dependencies = self._extract_dependencies(sql_content)
        
        return SQLObject(
            name=object_name,
            object_type=obj_type,
            sql_content=sql_content,
            start_line=start_line,
            end_line=end_line,
            file_path=file_path,
            dependencies=dependencies
        )

    def _find_object_end(self, content: str, start_pos: int, obj_type: SQLObjectType) -> int:
        """Find the end position of a SQL object."""
        
        # For procedures and functions, we need to handle BEGIN...END blocks
        if obj_type in [SQLObjectType.PROCEDURE, SQLObjectType.FUNCTION]:
            return self._find_procedure_end(content, start_pos)
        
        # For INSERT statements and other objects, find the next semicolon or end of file
        return self._find_semicolon_end(content, start_pos)

    def _find_procedure_end(self, content: str, start_pos: int) -> int:
        """Find the end of a procedure or function (handles BEGIN...END blocks)."""
        
        # Look for the opening of the procedure body
        body_start = content.find('AS', start_pos)
        if body_start == -1:
            body_start = content.find('LANGUAGE', start_pos)
        if body_start == -1:
            return self._find_semicolon_end(content, start_pos)
        
        # Find the actual body start (after AS or LANGUAGE)
        body_start = content.find('$$', body_start)
        if body_start == -1:
            body_start = content.find('BEGIN', body_start)
        if body_start == -1:
            return self._find_semicolon_end(content, start_pos)
        
        # Handle dollar-quoted strings
        if content[body_start:body_start+2] == '$$':
            return self._find_dollar_quote_end(content, body_start)
        
        # Handle BEGIN...END blocks
        return self._find_begin_end_end(content, body_start)

    def _find_dollar_quote_end(self, content: str, start_pos: int) -> int:
        """Find the end of a dollar-quoted string."""
        
        # Find the closing $$
        end_pos = content.find('$$', start_pos + 2)
        if end_pos == -1:
            return -1
        
        # Find the semicolon after the closing $$
        semicolon_pos = content.find(';', end_pos + 2)
        return semicolon_pos + 1 if semicolon_pos != -1 else end_pos + 2

    def _find_begin_end_end(self, content: str, start_pos: int) -> int:
        """Find the end of a BEGIN...END block."""
        
        begin_count = 0
        i = start_pos
        
        while i < len(content):
            if content[i:i+5].upper() == 'BEGIN':
                begin_count += 1
                i += 5
            elif content[i:i+3].upper() == 'END':
                begin_count -= 1
                if begin_count == 0:
                    # Found matching END, look for semicolon
                    semicolon_pos = content.find(';', i + 3)
                    return semicolon_pos + 1 if semicolon_pos != -1 else i + 3
                i += 3
            else:
                i += 1
        
        return -1

    def _find_semicolon_end(self, content: str, start_pos: int) -> int:
        """Find the next semicolon that's not inside a string literal."""
        
        in_single_quote = False
        in_double_quote = False
        escape_next = False
        
        i = start_pos
        while i < len(content):
            char = content[i]
            
            if escape_next:
                escape_next = False
                i += 1
                continue
            
            if char == '\\':
                escape_next = True
                i += 1
                continue
            
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif char == ';' and not in_single_quote and not in_double_quote:
                return i + 1
            
            i += 1
        
        return len(content)

    def _extract_object_name(self, sql_content: str, obj_type: SQLObjectType) -> str:
        """Extract the name of the SQL object."""
        
        # Remove comments and normalize whitespace
        clean_sql = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
        clean_sql = re.sub(r'/\*.*?\*/', '', clean_sql, flags=re.DOTALL)
        clean_sql = ' '.join(clean_sql.split())
        
        # Extract name based on object type
        for pattern in self.compiled_patterns[obj_type]:
            match = pattern.search(clean_sql)
            if match:
                # Get the text after the CREATE/ALTER/DROP/INSERT statement
                remaining = clean_sql[match.end():].strip()
                
                # For procedures and functions, look for the name before the opening parenthesis
                if obj_type in [SQLObjectType.PROCEDURE, SQLObjectType.FUNCTION]:
                    # Find the opening parenthesis
                    paren_pos = remaining.find('(')
                    if paren_pos != -1:
                        name_part = remaining[:paren_pos].strip()
                    else:
                        name_part = remaining
                # For INSERT statements, extract table name before VALUES or column list
                elif obj_type == SQLObjectType.INSERT:
                    # Look for VALUES keyword or opening parenthesis for column list
                    values_pos = remaining.find('VALUES')
                    paren_pos = remaining.find('(')
                    
                    if values_pos != -1 and (paren_pos == -1 or values_pos < paren_pos):
                        name_part = remaining[:values_pos].strip()
                    elif paren_pos != -1:
                        name_part = remaining[:paren_pos].strip()
                    else:
                        name_part = remaining
                else:
                    name_part = remaining
                
                # Split by whitespace and take the first part (the name)
                parts = name_part.split()
                if parts:
                    name = parts[0]
                    # Remove any trailing punctuation but keep underscores and dots
                    name = re.sub(r'[^\w._]', '', name)
                    return name
        
        return f"unnamed_{obj_type.value}_{hash(sql_content) % 10000}"

    def _extract_dependencies(self, sql_content: str) -> List[str]:
        """Extract dependencies (table/view references) from SQL content."""
        
        dependencies = []
        
        # Look for table/view references in FROM, JOIN, etc.
        patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'UPDATE\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'INTO\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql_content, re.IGNORECASE)
            dependencies.extend(matches)
        
        # Remove duplicates and clean up
        return list(set(dep.strip() for dep in dependencies if dep.strip()))

    def _remove_overlapping_objects(self, objects: List[SQLObject]) -> List[SQLObject]:
        """Remove overlapping objects, keeping the first one."""
        
        if not objects:
            return objects
        
        filtered = [objects[0]]
        
        for obj in objects[1:]:
            # Check if this object overlaps with any already included
            overlaps = False
            for existing in filtered:
                if (obj.start_line <= existing.end_line and 
                    obj.end_line >= existing.start_line):
                    overlaps = True
                    break
            
            if not overlaps:
                filtered.append(obj)
        
        return filtered

    def parse_folder(self, folder_path: str) -> Dict[SQLObjectType, List[SQLObject]]:
        """
        Parse all SQL files in a folder and organize by object type.
        
        Args:
            folder_path: Path to folder containing SQL files
            
        Returns:
            Dictionary mapping object types to lists of SQL objects
        """
        result = {obj_type: [] for obj_type in SQLObjectType}
        
        if not os.path.exists(folder_path):
            print(f"Folder not found: {folder_path}")
            return result
        
        sql_files = [f for f in os.listdir(folder_path) if f.endswith('.sql')]
        
        if not sql_files:
            print(f"No SQL files found in {folder_path}")
            return result
        
        print(f"Found {len(sql_files)} SQL files to parse")
        
        for sql_file in sql_files:
            file_path = os.path.join(folder_path, sql_file)
            print(f"Parsing {sql_file}...")
            
            objects = self.parse_file(file_path)
            
            for obj in objects:
                result[obj.object_type].append(obj)
                print(f"  Found {obj.object_type.value}: {obj.name}")
        
        return result

import ace_lib
from ace_lib import get_operators
import ast
import re
from typing import Tuple
import pandas as pd

class AlphaExpressionValidator(ast.NodeVisitor):
    """
    遍历 AST 并根据平台算子定义验证表达式。
    """
    def __init__(self, session, alpha_type='REGULAR', operators_df=None):
        
        self.session = session
        # 使用传入的 operators_df，如果未传入则回退到内部获取（兼容旧调用，但建议传入以避免网络风险）
        if operators_df is not None:
             self.operators_df = operators_df
        else:
             self.operators_df = get_operators(self.session)
             
        self.alpha_type = alpha_type
        self.operators = self._parse_operator_definitions()
        self.errors = []

    def _parse_operator_definitions(self):
        """从DataFrame解析原始算子定义，提取参数信息。"""
        
        # 过滤得到适用于特定alpha类型的算子
        def filter_scope(scope_list):
            return self.alpha_type in scope_list

        scoped_operators_df = self.operators_df[self.operators_df['scope'].apply(filter_scope)].copy()

        parsed_defs = {}
        for index, op_def in scoped_operators_df.iterrows():
            name = op_def['name']
            # 从'definition'字段中提取函数签名
            signature_match = re.match(r"([\w_]+)\s*\((.*?)\)", op_def['definition'])
            if not signature_match:
                continue

            func_name, params_str = signature_match.groups()

            # 对 'winsorize' 的特殊处理，因为 'std' 参数是必需的
            if name == 'winsorize':
                parsed_defs[name] = {
                    'required_args': 2,
                    'optional_args': 0,
                    'total_args': 2,
                    'keyword_args': {'std'},
                    'has_variable_args': False,
                    'min_args': 2
                }
                continue

            if name == 'ts_rank':
                parsed_defs[name] = {
                    'required_args': 2,
                    'optional_args': 0,
                    'total_args': 2,
                    'keyword_args': set(),
                    'has_variable_args': False,
                    'min_args': 2
                }
                continue

            params = [p.strip() for p in params_str.split(',')] if params_str else []

            required_args = 0
            optional_args = 0
            keyword_args = set()
            has_variable_args = '...' in params_str or '..' in params_str

            for param in params:
                if '=' in param:
                    optional_args += 1
                    keyword_args.add(param.split('=')[0].strip())
                elif '...' not in param and '..' not in param:
                    required_args += 1
            
            description = op_def.get('description', '')
            min_args_match = re.search(r"at least (\d+) inputs required", description) if isinstance(description, str) else None
            min_args = int(min_args_match.group(1)) if min_args_match else None

            parsed_defs[name] = {
                'required_args': required_args,
                'optional_args': optional_args,
                'total_args': required_args + optional_args,
                'keyword_args': keyword_args,
                'has_variable_args': has_variable_args,
                'min_args': min_args
            }
        
        # Manually add definitions for comparison and logical operators as they are not always 
        # in the operators list but are valid functions in WorldQuant BRAIN.
        for op_name in ['greater', 'less', 'greater_equal', 'less_equal', 'equal', 'not_equal']:
            parsed_defs[op_name] = {
                'required_args': 2,
                'optional_args': 0,
                'total_args': 2,
                'keyword_args': set(),
                'has_variable_args': False,
                'min_args': 2
            }
        
        for op_name in ['and', 'or']:
            parsed_defs[op_name] = {
                'required_args': 2,
                'optional_args': 0,
                'total_args': 2,
                'keyword_args': set(),
                'has_variable_args': True,
                'min_args': 2
            }
            
        parsed_defs['not'] = {
            'required_args': 1,
            'optional_args': 0,
            'total_args': 1,
            'keyword_args': set(),
            'has_variable_args': False,
            'min_args': 1
        }

        # Manually override definitions for 'add', 'multiply', and 'subtract'
        # to support variadic arguments (for add/multiply) and the 'filter' keyword.
        for op in ['add', 'multiply']:
            if op in parsed_defs:
                parsed_defs[op]['has_variable_args'] = True
                parsed_defs[op]['keyword_args'].add('filter')
                if parsed_defs[op]['min_args'] is None:
                     parsed_defs[op]['min_args'] = 2

        if 'subtract' in parsed_defs:
            parsed_defs['subtract']['keyword_args'].add('filter')
            # Increase total_args limit to accommodate filter argument, as subtract is not variadic
            parsed_defs['subtract']['total_args'] += 1
            
        return parsed_defs

    def visit_Call(self, node):
        """访问函数调用节点。先访问子节点，再验证当前节点（后序遍历）。"""
        # Step 1: 深入访问所有子节点。
        # 这确保在验证当前调用之前，所有嵌套的表达式（如参数）都已被验证。
        self.generic_visit(node)

        # Step 2: 验证当前函数调用节点。
        if isinstance(node.func, ast.Name):
            op_name = node.func.id
            
            # Map back functional forms of and/or renamed in check_expression_syntax
            if op_name == '__brain_and__':
                op_name = 'and'
            elif op_name == '__brain_or__':
                op_name = 'or'
            
            if op_name in self.operators:
                op_def = self.operators[op_name]
                pos_args_count = len(node.args)
                keyword_args_count = len(node.keywords)
                total_args_count = pos_args_count + keyword_args_count

                # --- 特殊验证规则 ---

                if op_name == 'ts_regression':
                    if not (3 <= total_args_count <= 5):
                        self.errors.append(f"Line {node.lineno}: '{op_name}' requires 3 to 5 arguments, but got {total_args_count}.")
                    # 验证从第3个位置开始的参数
                    for i in range(2, pos_args_count):
                        arg = node.args[i]
                        if not (isinstance(arg, ast.Constant) and isinstance(arg.value, int) and arg.value >= 0):
                            self.errors.append(f"Line {node.lineno}: Argument {i+1} of '{op_name}' must be a non-negative integer constant.")
                    # 如果参数超过3个，验证最后一个参数的值
                    if total_args_count > 3:
                        last_arg_node = node.args[-1] if pos_args_count == total_args_count else node.keywords[-1].value
                        if isinstance(last_arg_node, ast.Constant) and isinstance(last_arg_node.value, int):
                            if not (0 <= last_arg_node.value <= 9):
                                self.errors.append(f"Line {node.lineno}: The last argument of '{op_name}' must be between 0 and 9.")
                
                elif op_name == 'winsorize':
                    if total_args_count != 2:
                        self.errors.append(f"Line {node.lineno}: '{op_name}' requires exactly 2 arguments, but got {total_args_count}.")
                    elif pos_args_count == 2:
                        self.errors.append(f"Line {node.lineno}: The second argument of '{op_name}' must be a keyword argument 'std' (e.g., std=3).")
                    elif keyword_args_count == 1 and node.keywords[0].arg == 'std':
                        second_arg_node = node.keywords[0].value
                        if not (isinstance(second_arg_node, ast.Constant) and isinstance(second_arg_node.value, (int, float)) and second_arg_node.value > 0):
                            self.errors.append(f"Line {node.lineno}: The 'std' argument of '{op_name}' must be a positive number.")
                    else:
                        self.errors.append(f"Line {node.lineno}: Invalid arguments for '{op_name}'. Expecting `winsorize(expression, std=positive_integer)`.")

                elif op_name == 'group_backfill':
                    is_valid = False
                    if total_args_count == 3:
                        arg3 = node.args[2] if pos_args_count == 3 else None
                        if arg3 and isinstance(arg3, ast.Constant) and isinstance(arg3.value, (int, float)) and arg3.value > 0:
                            is_valid = True
                    elif total_args_count == 4:
                        if pos_args_count == 4:
                            arg3, arg4 = node.args[2], node.args[3]
                            if (isinstance(arg3, ast.Constant) and isinstance(arg3.value, (int, float)) and arg3.value > 0 and
                                isinstance(arg4, ast.Constant) and isinstance(arg4.value, (int, float)) and arg4.value > 0):
                                is_valid = True
                        elif pos_args_count == 3 and keyword_args_count == 1 and node.keywords[0].arg == 'std':
                             arg3 = node.args[2]
                             arg4_val = node.keywords[0].value
                             if (isinstance(arg3, ast.Constant) and isinstance(arg3.value, (int, float)) and arg3.value > 0 and
                                 isinstance(arg4_val, ast.Constant) and isinstance(arg4_val.value, (int, float)) and arg4_val.value > 0):
                                 is_valid = True
                    
                    if not is_valid and not (3 <= total_args_count <= 4):
                         self.errors.append(f"Line {node.lineno}: '{op_name}' requires 3 or 4 arguments, but got {total_args_count}.")
                    elif not is_valid:
                        self.errors.append(f"Line {node.lineno}: Invalid arguments for '{op_name}'. Check if the last one/two arguments are positive integers or if the 4th argument is a named 'std'.")

                elif op_name == 'ts_backfill':
                    if len(node.args) > 1 and not isinstance(node.args[1], ast.Constant):
                        self.errors.append(
                            f"Line {node.lineno}: The second argument (days) of 'ts_backfill' must be an integer constant."
                        )

                elif op_name == 'quantile':
                    if not (1 <= total_args_count <= 3):
                        self.errors.append(f"Line {node.lineno}: '{op_name}' requires 1 to 3 arguments, but got {total_args_count}.")
                    
                    for kw in node.keywords:
                        if kw.arg == 'driver':
                            if not (isinstance(kw.value, ast.Constant) and kw.value.s in ['gaussian', 'uniform', 'cauchy']):
                                self.errors.append(f"Line {node.lineno}: 'driver' for '{op_name}' must be one of 'gaussian', 'uniform', or 'cauchy'.")
                        elif kw.arg == 'sigma':
                            if not (isinstance(kw.value, ast.Constant) and isinstance(kw.value.n, (int, float)) and kw.value.n > 0):
                                self.errors.append(f"Line {node.lineno}: 'sigma' for '{op_name}' must be a positive number.")
                        else:
                            self.errors.append(f"Line {node.lineno}: Unknown keyword argument '{kw.arg}' for '{op_name}'.")

                elif op_name.startswith('vec_'):
                    if len(node.args) != 1:
                        self.errors.append(
                            f"Line {node.lineno}: Operator '{op_name}' must have exactly one argument, but {len(node.args)} were provided."
                        )
                    elif not isinstance(node.args[0], ast.Name):
                        self.errors.append(
                            f"Line {node.lineno}: Operator '{op_name}' must take a single datafield as an argument, but an expression or constant was provided."
                        )
                
                elif op_name == 'hump':
                    if len(node.args) > 1:
                        second_arg = node.args[1]
                        if isinstance(second_arg, ast.Constant):
                            self.errors.append(
                                f"Line {node.lineno}: The second argument of 'hump' is a constant and must be passed as a keyword argument (e.g., hump={second_arg.value})."
                            )
                
                # --- 通用验证 ---
                else:
                    for kw in node.keywords:
                        if kw.arg not in op_def['keyword_args']:
                            self.errors.append(
                                f"Line {node.lineno}: Operator '{op_name}' does not accept keyword argument '{kw.arg}'."
                            )

                    # Check for positional arguments that should be keyword arguments
                    if not op_def['has_variable_args']:
                        required_args_count = op_def['required_args']
                        for i, arg in enumerate(node.args):
                            if i >= required_args_count:
                                if isinstance(arg, ast.Constant):
                                    # This is a positional argument for an optional parameter, and it's a constant.
                                    # This should be a keyword argument.
                                    self.errors.append(
                                        f"Line {node.lineno}: Positional argument {i + 1} of '{op_name}' is a constant and must be passed as a keyword argument."
                                    )

                    if op_def['has_variable_args']:
                        min_args = op_def['min_args'] or op_def['required_args']
                        if total_args_count < min_args:
                            self.errors.append(
                                f"Line {node.lineno}: Operator '{op_name}' requires at least {min_args} arguments, but only {total_args_count} were provided."
                            )
                    else:
                        if total_args_count < op_def['required_args']:
                             self.errors.append(
                                f"Line {node.lineno}: Operator '{op_name}' requires {op_def['required_args']} mandatory arguments, but only {total_args_count} were provided."
                            )
                        if total_args_count > op_def['total_args']:
                            self.errors.append(
                                f"Line {node.lineno}: Operator '{op_name}' accepts at most {op_def['total_args']} arguments, but {total_args_count} were provided."
                            )
            else:
                self.errors.append(f"Line {node.lineno}: Unknown operator or operator not supported for current Alpha type '{op_name}'.")



def check_custom_rules(expression: str) -> Tuple[bool, list]:
    """
    Checks for specific function parameter rules that are hard to verify with AST alone.
    This function is being deprecated in favor of more robust AST checks in `visit_Call`,
    but is kept for potential future use with other complex string-based rules.
    """
    errors = []

    # All previous rules for winsorize, group_backfill, and and ts_regression
    # have been moved to the visit_Call method for more robust AST-based validation.

    if errors:
        return False, list(set(errors)) # Return unique errors
    return True, []


def check_expression_syntax(expression: str, session, alpha_type: str = 'REGULAR', operators_df=None) -> Tuple[bool, list]:
    """
    使用 AST 和平台算子定义来详细检查表达式的语法和语义。

    Args:
        expression (str): 需要被检查的 alpha 表达式字符串。
        session: Ace session object.
        alpha_type (str): Alpha 的类型 ('REGULAR', 'COMBO', 'SELECTION')，用于筛选适用的算子。
        operators_df (pd.DataFrame, optional): 预先获取的算子 DataFrame。如果提供，将避免在内部进行网络请求。

    Returns:
        Tuple[bool, list]: 一个元组，第一个元素是布尔值，表示是否验证通过；
                           第二个元素是一个列表，包含所有发现的错误信息。
    """
    # Step 1: Run custom rule checks first
    is_custom_valid, custom_errors = check_custom_rules(expression)
    if not is_custom_valid:
        return False, custom_errors

    # Step 2: If custom rules pass, proceed with AST validation
    # Brain platform requires '&&' and '||', so we replace them for Python's AST parser.
    # We also explicitly disallow single '&' and '|'.
    if re.search(r'(?<!&)&(?!&)', expression):
        return False, ["Syntax error: Single '&' is not allowed. Use '&&' for logical AND."]
    if re.search(r'(?<!\|)\|(?!\|)', expression):
        return False, ["Syntax error: Single '|' is not allowed. Use '||' for logical OR."]

    # Brain platform supports both operators (&&, ||) and functional forms (and(), or(), not()).
    # Functional forms conflict with Python keywords, so we rename them for parsing.
    processed_expression = re.sub(r'\band\s*\(', '__brain_and__(', expression)
    processed_expression = re.sub(r'\bor\s*\(', '__brain_or__(', processed_expression)
    processed_expression = re.sub(r'\bnot\s*\(', '__brain_not__(', processed_expression)
    
    # Brain platform uses lowercase true/false, map them to Python's True/False for AST compatibility
    processed_expression = re.sub(r'\btrue\b', 'True', processed_expression)
    processed_expression = re.sub(r'\bfalse\b', 'False', processed_expression)
    
    processed_expression = processed_expression.replace('&&', ' and ').replace('||', ' or ')

    try:
        tree = ast.parse(processed_expression)
    except SyntaxError as e:
        # After replacement, a syntax error points to a different issue in the expression.
        return False, [f"Basic syntax error: {e} (after processing '&&'/'||')"]

    validator = AlphaExpressionValidator(session=session, alpha_type=alpha_type, operators_df=operators_df)
    validator.visit(tree)

    if validator.errors:
        return False, validator.errors
    else:
        return True, []

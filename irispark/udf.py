from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable
from typing import (
    TYPE_CHECKING,
    Any,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

if TYPE_CHECKING:
    from irispark.session import IrisParkSession
    from irispark.types import DataType


def _infer_iris_sql_type(annotation: type) -> str:
    """Map a Python type annotation to IRIS SQL type string."""
    # Handle Optional/Union (both typing.Union and Python 3.10+ | syntax)
    origin = get_origin(annotation)
    if origin is Union:
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _infer_iris_sql_type(non_none[0])
    # Python 3.10+ | syntax
    try:
        if origin is type(int | str):
            args = get_args(annotation)
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return _infer_iris_sql_type(non_none[0])
    except TypeError:
        pass

    # Handle typing types
    origin = get_origin(annotation)
    if origin is list:
        return "ARRAY"
    if origin is dict:
        return "MAP"

    # Primitive types
    type_map = {
        str: "VARCHAR(4000)",
        int: "INTEGER",
        float: "DOUBLE",
        bool: "BOOLEAN",
        bytes: "VARBINARY",
    }

    import datetime
    if annotation is datetime.date:
        return "DATE"
    if annotation is datetime.datetime:
        return "TIMESTAMP"
    if annotation is datetime.time:
        return "TIME"

    if annotation in type_map:
        return type_map[annotation]

    return "VARCHAR(4000)"


def _infer_signature(fn: Callable) -> tuple[list[tuple[str, str]], str]:
    """Infer the SQL function signature from a Python callable."""
    sig = inspect.signature(fn)
    type_hints = get_type_hints(fn)

    params = []
    for name, param in sig.parameters.items():
        if name == "context":
            continue
        annotation = type_hints.get(name, str)
        iris_sql_type = _infer_iris_sql_type(annotation)
        params.append((name, iris_sql_type))

    return_annotation = type_hints.get("return", str)
    return_type_sql = _infer_iris_sql_type(return_annotation)

    return params, return_type_sql


def _extract_function_body(fn: Callable) -> str:
    """Extract the function body from a callable, removing the def line and dedenting."""
    source = inspect.getsource(fn)
    lines = source.splitlines()
    def_idx = next(i for i, line in enumerate(lines) if line.lstrip().startswith("def "))
    body_lines = lines[def_idx + 1:]
    return textwrap.dedent("\n".join(body_lines)).strip()


def _generate_create_function_ddl(
    name: str,
    params: list[tuple[str, str]],
    return_type: str,
    fn: Callable,
    deterministic: bool = True,
) -> str:
    """Generate CREATE FUNCTION DDL for IRIS Embedded Python."""
    param_list = ", ".join(f"{name} {sql_type}" for name, sql_type in params)
    body = _extract_function_body(fn)
    det = "DETERMINISTIC" if deterministic else "NOT DETERMINISTIC"

    prefix = (
        f"CREATE OR REPLACE FUNCTION {name} ({param_list}) "
        f"RETURNS {return_type} LANGUAGE PYTHON {det} "
    )
    return prefix + "{\n" + body + "\n};"


def _generate_drop_function_sql(name: str) -> str:
    return f"DROP FUNCTION IF EXISTS {name}"


class ObjectScriptGenerator(ast.NodeVisitor):
    """Convert simple Python AST to ObjectScript for IRIS UDFs."""

    def __init__(self, param_names: list[str], target_function: str):
        self.param_names = param_names
        self.target_function = target_function
        self.result: list[str] = []
        self.indent = 0
        self.errors: list[str] = []

    def _emit(self, line: str):
        self.result.append("    " * self.indent + line)

    def _type_of(self, node: ast.AST) -> str:
        return "unknown"

    def _is_likely_string(self, node: ast.AST) -> bool:
        """Heuristic to detect if an expression is likely a string."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return True
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            # Recursive check for string concatenation
            return self._is_likely_string(node.left) or self._is_likely_string(node.right)
        if isinstance(node, ast.Call):
            # String method calls or functions returning strings
            return True
        return False

    def visit_FunctionDef(self, node: ast.FunctionDef):
        if node.name == self.target_function:
            for stmt in node.body:
                self.visit(stmt)
        else:
            self.errors.append("Unsupported: nested function definition")

    def visit_Return(self, node: ast.Return):
        if node.value:
            expr = self._expr_to_os(node.value)
            self._emit(f"Quit {expr}")
        else:
            self._emit("Quit")

    def visit_Assign(self, node: ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name):
                var_name = target.id
                value = self._expr_to_os(node.value)
                self._emit(f"Set {var_name} = {value}")

    def visit_If(self, node: ast.If):
        test = self._expr_to_os(node.test)
        self._emit(f"If {test} {{")
        self.indent += 1
        for stmt in node.body:
            self.visit(stmt)
        self.indent -= 1
        self._emit("}")
        if node.orelse:
            self._emit("Else {")
            self.indent += 1
            for stmt in node.orelse:
                self.visit(stmt)
            self.indent -= 1
            self._emit("}")

    def visit_Expr(self, node: ast.Expr):
        # Expression statement (e.g., function call)
        self._expr_to_os(node.value)

    def visit_For(self, node: ast.For):
        self.errors.append("Unsupported statement: for loop")

    def visit_While(self, node: ast.While):
        self.errors.append("Unsupported statement: while loop")

    def visit_Try(self, node: ast.Try):
        self.errors.append("Unsupported statement: try/except")

    def visit_With(self, node: ast.With):
        self.errors.append("Unsupported statement: with")

    def visit_Match(self, node: ast.Match):
        self.errors.append("Unsupported statement: match")

    def visit_ClassDef(self, node: ast.ClassDef):
        self.errors.append("Unsupported statement: class definition")

    def _expr_to_os(self, node: ast.AST) -> str:
        if isinstance(node, ast.Constant):
            if node.value is None:
                return '""'
            if isinstance(node.value, str):
                escaped = node.value.replace('"', '""')
                return f'"{escaped}"'
            return str(node.value)
        elif isinstance(node, ast.Name):
            if node.id in self.param_names:
                return node.id
            return node.id
        elif isinstance(node, ast.BinOp):
            left = self._expr_to_os(node.left)
            right = self._expr_to_os(node.right)
            op_map = {
                ast.Add: "+",
                ast.Sub: "-",
                ast.Mult: "*",
                ast.Div: "/",
                ast.Mod: "#",
                ast.Pow: "**",
            }
            # String concatenation in ObjectScript uses _
            if isinstance(node.op, ast.Add):
                left_is_str = self._is_likely_string(node.left)
                right_is_str = self._is_likely_string(node.right)
                if left_is_str or right_is_str:
                    op = "_"
                else:
                    op = "+"
            else:
                op = op_map.get(type(node.op), "?")
            return f"{left} {op} {right}"
        elif isinstance(node, ast.Compare):
            left = self._expr_to_os(node.left)
            ops = []
            for cmp_op, comp in zip(node.ops, node.comparators):
                right = self._expr_to_os(comp)
                cmp_map: dict[type[ast.cmpop], str] = {
                    ast.Eq: "=",
                    ast.NotEq: "'=",
                    ast.Lt: "<",
                    ast.LtE: "<=",
                    ast.Gt: ">",
                    ast.GtE: ">=",
                    ast.Is: "=",
                    ast.IsNot: "'=",
                    ast.In: "[",
                    ast.NotIn: "'[",
                }
                ops.append(f"{left} {cmp_map.get(type(cmp_op), '?')} {right}")
            return " && ".join(ops)
        elif isinstance(node, ast.BoolOp):
            values = [self._expr_to_os(v) for v in node.values]
            op = " && " if isinstance(node.op, ast.And) else " || "
            return op.join(values)
        elif isinstance(node, ast.UnaryOp):
            operand = self._expr_to_os(node.operand)
            if isinstance(node.op, ast.Not):
                return f"'{operand}'"
            if isinstance(node.op, ast.USub):
                return f"-{operand}"
            return operand
        elif isinstance(node, ast.IfExp):
            test = self._expr_to_os(node.test)
            body = self._expr_to_os(node.body)
            orelse = self._expr_to_os(node.orelse)
            return f"$SELECT({test}:{body},1:{orelse})"
        elif isinstance(node, ast.Call):
            func_name = ""
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr
            args = ", ".join(self._expr_to_os(arg) for arg in node.args)
            return f"${func_name}({args})"
        elif isinstance(node, ast.Subscript):
            value = self._expr_to_os(node.value)
            if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, int):
                return f"$PIECE({value}, \",\", {node.slice.value + 1})"
            return value
        elif isinstance(node, ast.List):
            elements = [self._expr_to_os(e) for e in node.elts]
            return ", ".join(elements)
        else:
            self.errors.append(f"Unsupported AST node: {type(node).__name__}")
            return "0"

    def generate(self) -> str:
        return "\n".join(self.result)


def _try_generate_objectscript(fn: Callable, param_names: list[str]) -> str | None:
    """Try to generate ObjectScript from Python function. Returns None if not possible."""
    try:
        source = inspect.getsource(fn)
        tree = ast.parse(source)
        # Find the function definition
        func_node = None
        for node in tree.body:
            if isinstance(node, ast.FunctionDef):
                func_node = node
                break
        if not func_node:
            return None

        generator = ObjectScriptGenerator(param_names, func_node.name)
        generator.visit(func_node)
        if generator.errors:
            return None
        return generator.generate()
    except Exception:
        return None


class UDFRegistration:
    """Public UDF registration interface."""

    def __init__(self, session: IrisParkSession) -> None:
        self._session = session
        self._registry: dict[str, Callable] = {}
        self._objectscript_udfs: set[str] = set()

    def register(
        self,
        name: str,
        fn: Callable | None = None,
        returnType: DataType | None = None,
    ) -> Callable:
        """
        Register a Python function as an IRIS SQL function.

        Usage:
            # Direct call
            spark.udf.register("my_udf", lambda x: x * 2, IntegerType())

            # Decorator
            @spark.udf.register("my_udf", IntegerType())
            def my_udf(x: int) -> int:
                return x * 2

        Tries to register as ObjectScript SQL function for pushdown.
        Falls back to Python-side execution if not possible.
        """
        if fn is None:
            def decorator(f: Callable) -> Callable:
                return self.register(name, f, returnType)
            return decorator

        from .types import DataType
        if isinstance(fn, DataType) and returnType is None:
            returnType = fn
            return self.register(name, None, returnType)

        # Infer signature
        params, return_type_sql = _infer_signature(fn)

        # Get function source
        try:
            inspect.getsource(fn)
        except OSError:
            self._register_fallback(fn.__name__, fn)
            self._registry[fn.__name__] = fn
            return fn

        # Try ObjectScript DDL first (for SQL pushdown)
        param_names = [p[0] for p in params]
        os_body = _try_generate_objectscript(fn, param_names)
        if os_body:
            try:
                self._try_objectscript_registration(name, params, return_type_sql, os_body)
                self._objectscript_udfs.add(name)
                self._registry[name] = fn
                return fn
            except Exception:
                pass  # Fall through to fallback

        # Try Embedded Python DDL
        try:
            self._try_embedded_python_registration(name, params, return_type_sql, fn)
            self._registry[name] = fn
            return fn
        except Exception:
            pass

        # Fall back to Python-side execution
        self._register_fallback(name, fn)
        self._registry[name] = fn

        return fn

    def _try_objectscript_registration(
        self, name: str, params: list[tuple[str, str]], return_type_sql: str, os_body: str
    ) -> None:
        """Register as ObjectScript SQL function."""
        param_list = ", ".join(f"{name} {sql_type}" for name, sql_type in params)
        ddl = (
            f"CREATE OR REPLACE FUNCTION {name} ({param_list}) "
            f"RETURNS {return_type_sql} LANGUAGE OBJECTSCRIPT {{\n{os_body}\n}};"
        )
        self._session.sql(ddl)

    def _try_embedded_python_registration(
        self, name: str, params: list[tuple[str, str]], return_type_sql: str, fn: Callable
    ) -> None:
        """Attempt to register as Embedded Python SQL function."""
        if not self._check_embedded_python():
            raise RuntimeError("Embedded Python not available")

        ddl = _generate_create_function_ddl(fn.__name__, params, return_type_sql, fn)
        self._session.sql(ddl)

    def _register_fallback(self, name: str, fn: Callable) -> None:
        """Register as fallback Python-side UDF (executes in Python process)."""
        self._registry[fn.__name__] = fn

    def execute_fallback(self, name: str, *args: Any) -> Any:
        """Execute a fallback UDF in the Python process."""
        fn = self._registry.get(name)
        if fn is None:
            raise ValueError(f"UDF '{name}' not registered")
        return fn(*args)

    def is_pushdown(self, name: str) -> bool:
        """Check if UDF executes in IRIS (pushdown) vs Python fallback."""
        return name in self._objectscript_udfs

    def _check_embedded_python(self) -> bool:
        """Check if Embedded Python is available for CREATE FUNCTION."""
        try:
            self._session.sql(
                "CREATE FUNCTION __irispark_epy_probe__() RETURNS INT "
                "LANGUAGE PYTHON { return 1 };"
            )
            self._session.sql("DROP FUNCTION __irispark_epy_probe__")
            return True
        except Exception:
            return False

    def _get(self, name: str) -> Callable | None:
        return self._registry.get(name)


# For backwards compatibility
_UDFRegistrationImpl = UDFRegistration

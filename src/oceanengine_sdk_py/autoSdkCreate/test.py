# -*- coding: utf-8 -*-
# 定义模板字符串
template = """
def {function_name}({parameters}):
    {body}
"""

# 填充模板
function_name = "add"
parameters = "a, b"
body = "return a + b"

# 生成代码字符串
generated_code = template.format(function_name=function_name, parameters=parameters, body=body)

# 打印生成的代码
print(generated_code)

# 执行生成的代码
exec(generated_code)

# 调用生成的函数
result = add(3, 5)
print("Result:", result)  # 输出: Result: 8

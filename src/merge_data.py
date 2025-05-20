import csv
import os


def merge_csv_files(file_a_path, file_b_path, mode='prepend'):
    """
    合并两个 CSV 文件，并将结果保存为 A 的文件名。

    Args:
        file_a_path (str): 文件 A 的路径。
        file_b_path (str): 文件 B 的路径。
        mode (str): 合并模式，'prepend' 表示 B 插在 A 前面，'append' 表示 B 追加在 A 后面。默认为 'prepend'。

    Raises:
        FileNotFoundError: 如果文件 A 或文件 B 不存在。
        ValueError: 如果 mode 参数值无效。
        RuntimeError: 如果合并过程中发生其他错误。
    """
    # 验证 mode 参数
    if mode not in ['prepend', 'append']:
        raise ValueError("mode 参数必须是 'prepend' 或 'append'")

    # 检查文件是否存在
    if not os.path.exists(file_a_path):
        raise FileNotFoundError(f"文件 A 不存在：{file_a_path}")
    if not os.path.exists(file_b_path):
        raise FileNotFoundError(f"文件 B 不存在：{file_b_path}")

    # 临时文件路径（与 A 文件同目录）
    temp_file_path = os.path.join(os.path.dirname(file_a_path), 'temp_merged.csv')

    try:
        # 打开文件 A 和文件 B
        with open(file_a_path, 'r', newline='') as file_a, \
             open(file_b_path, 'r', newline='') as file_b, \
             open(temp_file_path, 'w', newline='') as temp_file:

            # 创建 CSV 读取器和写入器
            reader_a = csv.DictReader(file_a)
            reader_b = csv.DictReader(file_b)
            fieldnames = reader_a.fieldnames  # 获取表头

            # 验证文件 B 的表头是否与文件 A 一致
            if reader_b.fieldnames != fieldnames:
                raise ValueError("文件 A 和文件 B 的表头不一致")

            writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
            writer.writeheader()  # 写入表头

            # 根据 mode 决定写入顺序
            if mode == 'prepend':
                # 先写入 B 的内容
                for row in reader_b:
                    writer.writerow(row)
                # 再写入 A 的内容
                for row in reader_a:
                    writer.writerow(row)
            else:  # mode == 'append'
                # 先写入 A 的内容
                for row in reader_a:
                    writer.writerow(row)
                # 再写入 B 的内容
                for row in reader_b:
                    writer.writerow(row)

        # 删除原文件 A
        os.remove(file_a_path)

        # 将临时文件重命名为 A 的文件名
        os.rename(temp_file_path, file_a_path)

        print(f"文件合并完成（模式：{mode}），结果已保存为：{file_a_path}")

    except Exception as e:
        # 如果发生错误，删除临时文件
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise RuntimeError(f"文件合并失败：{e}")

# 文件路径
file_a_path = os.path.join('/Users/sujianan/py/alpha_simulator_bot/data', 'alpha_list_pending_simulated.csv')
file_b_path = os.path.join('/Users/sujianan/py/consultant/consultant/output', 'alpha_list_pending_simulated.csv')

# 合并文件
merge_csv_files(file_a_path, file_b_path, 'append')
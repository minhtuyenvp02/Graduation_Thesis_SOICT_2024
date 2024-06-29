from minio import Minio
from collections import defaultdict
# Kết nối tới MinIO server
client = Minio(
    "146.190.107.105:30090",  # Thay bằng URL của MinIO server
    access_key="admin",  # Thay bằng access key
    secret_key="admin123",  # Thay bằng secret key
    cert_check=False,
    secure=False
)

# Hàm đệ quy để in cấu trúc cây thư mục
def print_tree(bucket_name, max_level=3):
    objects = client.list_objects(bucket_name, recursive=True)
    tree = defaultdict(set)

    # Xây dựng cây thư mục
    for obj in objects:
        parts = obj.object_name.split('/')
        for i in range(1, min(len(parts), max_level + 1)):
            tree['/'.join(parts[:i - 1])].add(parts[i - 1])

    def print_branch(prefix, level, indent, is_last):
        if level > max_level:
            return
        branches = sorted(tree[prefix])
        for index, branch in enumerate(branches):
            connector = '└── ' if index == len(branches) - 1 else '├── '
            print(f"{indent}{connector}{branch}/")
            next_prefix = f"{prefix}/{branch}" if prefix else branch
            if next_prefix in tree:
                next_indent = indent + ('    ' if index == len(branches) - 1 else '│   ')
                print_branch(next_prefix, level + 1, next_indent, index == len(branches) - 1)

    # Khởi động in cây từ thư mục gốc
    print_branch('', 1, '', True)
def print_tree2(bucket_name, prefix='', indent=''):
    objects = client.list_objects(bucket_name, prefix, recursive=True)
    for obj in objects:
        object_name = obj.object_name
        if object_name.endswith('/'):
            print(f"{indent}└── {object_name}")
            print_tree(bucket_name, prefix=object_name, indent=indent + '    ')
        else:
            print(f"{indent}├── {object_name}")
def print_tree3(bucket_name, prefix='', indent='', is_last=True):
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=False)
    dirs = []
    files = []

    for obj in objects:
        object_name = obj.object_name
        if object_name.endswith('/'):
            dirs.append(object_name)
        else:
            files.append(object_name)

    for i, dir_name in enumerate(dirs):
        last = (i == len(dirs) - 1) and (len(files) == 0)
        print(indent + ('└── ' if last else '├── ') + dir_name)
        if not last:
            print_tree3(bucket_name, prefix=prefix + dir_name, indent=indent + ('    ' if last else '│   '),
                       is_last=last)
        else:
            print_tree3(bucket_name, prefix=prefix + dir_name, indent=indent + '    ', is_last=last)

    for i, file_name in enumerate(files):
        last = (i == len(files) - 1)
        print(indent + ('└── ' if is_last and last else '├── ') + file_name)

# Liệt kê các đối tượng trong bucket và in cấu trúc cây thư mục
# Liệt kê các đối tượng trong bucket và in cấu trúc cây thư mục
bucket_name = "nyc-trip-bucket"  # Thay bằng tên bucket của bạn
print_tree3(bucket_name)

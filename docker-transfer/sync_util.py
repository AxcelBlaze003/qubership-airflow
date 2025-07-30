from ruamel.yaml import YAML
import re
import sys
import os

yaml = YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)

yaml.representer.add_representer(
    type(None), lambda self, _: self.represent_scalar('tag:yaml.org,2002:null', '~')
)

def parse_release_images_yaml(file_path):
    with open(file_path) as f:
        data = yaml.load(f)

    image_versions = {}
    for _, image_full in data.items():
        match = re.match(r"(.+):([\w.-]+)", image_full)
        if match:
            image_name, tag = match.groups()
            image_versions[image_name] = tag
    return image_versions

def update_node(node, image_versions):
    if isinstance(node, dict):
        if "registry" in node and "tag" in node and "repository" not in node:
            repo = node["registry"]
            old_tag = node["tag"]
            if repo in image_versions:
                new_tag = image_versions[repo]
                if new_tag != old_tag:
                    print(f"Updating tag for {repo}: {old_tag} → {new_tag}")
                    node["tag"] = new_tag
                else:
                    print(f"No change needed for {repo}: already {new_tag}")

        if "registry" in node and "repository" in node and "tag" in node:
            full_image = f"{node['registry'].rstrip('/')}/{node['repository'].lstrip('/')}"
            old_tag = node["tag"]
            if full_image in image_versions:
                new_tag = image_versions[full_image]
                if new_tag != old_tag:
                    print(f"Updating tag for {full_image}: {old_tag} → {new_tag}")
                    node["tag"] = new_tag
                else:
                    print(f"No change needed for {full_image}: already {new_tag}")

        if "repository" in node and "tag" in node:
            full_image = node["repository"]
            old_tag = node["tag"]
            if full_image in image_versions:
                new_tag = image_versions[full_image]
                if new_tag != old_tag:
                    print(f"Updating tag for {full_image}: {old_tag} → {new_tag}")
                    node["tag"] = new_tag
                else:
                    print(f"No change needed for {full_image}: already {new_tag}")            

        for key, value in node.items():
            node[key] = update_node(value, image_versions)

    elif isinstance(node, list):
        return [update_node(item, image_versions) for item in node]

    elif isinstance(node, str):
        match = re.match(r"^(.+):([\w.-]+)$", node)
        if match:
            image_name, current_tag = match.groups()
            if image_name in image_versions:
                new_tag = image_versions[image_name]
                if new_tag != current_tag:
                    print(f"Updating image: {node} → {image_name}:{new_tag}")
                    return f"{image_name}:{new_tag}"
        return node

    return node

def update_values_yaml(values_path, image_versions):
    if not os.path.exists(values_path):
        print(f"File not found: {values_path}")
        return

    with open(values_path) as f:
        values = yaml.load(f)

    updated = update_node(values, image_versions)

    with open(values_path, "w") as f:
        yaml.dump(updated, f)


def sync_chart_version_from_values(chart_path, values_path):
    if not os.path.exists(chart_path):
        print(f"Chart.yaml not found: {chart_path}")
        return

    if not os.path.exists(values_path):
        print(f"Values.yaml not found: {values_path}")
        return

    with open(values_path, "r") as f:
        values_data = yaml.load(f)

    try:
        image_tag = values_data["image"]["tag"]
    except KeyError:
        print("image.tag not found in values.yaml")
        return

    with open(chart_path, "r") as f:
        chart_data = yaml.load(f)

    old_version = chart_data.get("version")
    if old_version != image_tag:
        print(f"Syncing Chart.yaml version: {old_version} → {image_tag}")
        chart_data["version"] = image_tag
        with open(chart_path, "w") as f:
            yaml.dump(chart_data, f)
    else:
        print(f"Chart.yaml version already matches image tag: {image_tag}")


if __name__ == "__main__":
   
    # Modes: images, chart-version

    mode = sys.argv[1]

    if mode == "images":
        releases_file = sys.argv[2]
        values_file = sys.argv[3]
        image_versions = parse_release_images_yaml(releases_file)
        update_values_yaml(values_file, image_versions)

    elif mode == "chart-version":
        chart_file = sys.argv[2]
        values_file = sys.argv[3]
        sync_chart_version_from_values(chart_file, values_file)

    else:
        print("Invalid mode")
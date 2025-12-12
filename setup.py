from setuptools import find_packages, setup

setup(
    name="action-create-dataproduct",
    version="1.0",
    packages=find_packages(),
    install_requires=["acryl-datahub-actions"],
    entry_points={
        "datahub_actions.action.plugins": [
            "action-create-dataproduct=action_create_dataproduct.action:CreateDataproductAction"
        ]
    },
)
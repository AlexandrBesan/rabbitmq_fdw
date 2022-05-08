from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="rabbitmq_fdw",
    version="1.0.0",
    description="Rabbitmq Foreign Data Wrapper",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/AlexandrBesan/rabbitmq_fdw",
    author="Alexandr Besan",
    author_email="alexandr.besan@gmail.com",
    keywords="postgresql, fdw, multicorn",
    package_dir={"": "src"},
    #packages=["src"],
    packages=find_packages('src'),
    # packages=find_packages(include=['src']),
    python_requires=">=3.7, <4",
    install_requires=["pika"],
     entry_points={
         "console_scripts": [
             "rabbitmq_fdw=rabbitmq_fdw.main:main",
         ],
     },
    project_urls={
        "Bug Reports": "https://github.com/AlexandrBesan/rabbitmq_fdw/issues",
        "Funding":  "https://www.paypal.com/donate/?hosted_button_id=GFCHS2N76JCG2",
        "Source": "https://github.com/AlexandrBesan/rabbitmq_fdw",
    },
)
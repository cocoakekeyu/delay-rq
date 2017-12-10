from setuptools import setup


setup(name='delayrq',
      version='0.1.0',
      description='A delayed queues for RQ',
      url='http://github.com/cocoakekeyu/delay-rq',
      author='cocoakekeyu',
      author_email='cocoakekeyu@gmail.com',
      license='MIT',
      install_requires=[
          'rq'
      ],
      packages=['delayrq'],
      platforms=['all'],
      )

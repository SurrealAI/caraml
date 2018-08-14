import os.path as op
from symphony.addons import DockerBuilder

# docker push destination URL
UPSTREAM_URL_ROOT = 'us.gcr.io/surreal-dev-188523'


settings = {
    'temp_directory': '~/Temp/symphony',
    'context_directories': [
        {
            'path': 'ADD/YOUR/LOCAL/FOLDER/PATH/TO/SYMPHONY',
            'name': 'symphony',
            # Note, you need to put in your symphony directory to allow the builder to copy symphony from the correct directory
            'force_update': True,
        },
    ],
    'verbose': True,
    'dockerfile': 'Dockerfile',
}

builder = DockerBuilder.from_dict(settings)
builder.build()
upstream_url = op.join(UPSTREAM_URL_ROOT, 'symphony-demo')
builder.tag(upstream_url, 'latest')
builder.push(upstream_url, 'latest')

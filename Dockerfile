FROM lambci/lambda:python3.7

FROM lambci/lambda-base:build

ENV PATH=/var/lang/bin:$PATH \
  LD_LIBRARY_PATH=/var/lang/lib:$LD_LIBRARY_PATH \
  AWS_EXECUTION_ENV=AWS_Lambda_python3.7 \
  PKG_CONFIG_PATH=/var/lang/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/share/pkgconfig \
  PIPX_BIN_DIR=/var/lang/bin \
  PIPX_HOME=/var/lang/pipx

COPY --from=0 /var/runtime /var/runtime
COPY --from=0 /var/lang /var/lang
COPY --from=0 /var/rapid /var/rapid

# Add these as a separate layer as they get updated frequently
RUN pip install --upgrade pip && \
  pip install -U pip setuptools wheel --no-cache-dir && \
  pip install pipx --no-cache-dir && \
  pipx install virtualenv && \
  pipx install pipenv && \
  pipx install poetry==1.1.13 && \
  pipx install awscli==1.* && \
  pipx install aws-lambda-builders==1.18.0 && \
  pipx install aws-sam-cli==1.52.0

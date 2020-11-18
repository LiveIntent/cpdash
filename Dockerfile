#    Copyright 2020 Jonas Dahlbæk

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http:#www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


FROM golang:1.15

ARG goos=linux
ARG goarch=amd64

WORKDIR /opt/src/cpdash
COPY . .

RUN CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch go build ./cmd/cpdash

FROM scratch

COPY --from=0 /opt/src/cpdash/cpdash /usr/local/bin/cpdash

ENTRYPOINT [ "/usr/local/bin/cpdash" ]

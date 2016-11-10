#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

module Fluent
  module PluginHelper
    module Socket
      # stop     : [-]
      # shutdown : [-]
      # close    : [-]
      # terminate: [-]

      attr_reader :_sockets # for tests

      def socket_create
      end

      def socket_create_tcp
      end

      def socket_create_udp
      end

      # def socket_create_tls
      # end

      def initialize
        super
        @_sockets = []
      end
    end
  end
end
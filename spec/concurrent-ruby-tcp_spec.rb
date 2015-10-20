require 'concurrent-ruby-tcp'

def expect_block_to_be_done_in(time, &block)
  expect(Thread.new(&block).join(time)).not_to be_nil
end

module Concurrent
  module Edge
    # TCP_LOGGER.outputters = Log4r::Outputter.stdout
    describe Context do
      class ContextTest; include Context; end
      subject { ContextTest.new }

      describe '#call' do
        it 'calls methods in the current context' do
          expect(subject).to receive(:test_method).with(1, 2, 3).and_return(4)
          expect(subject.call(:test_method, 1, 2, 3)).to eq(4)
        end
        it 'calls methods on specified objects' do
          expect(subject.call([1, 2, 3], :+, [4, 5, 6])).to eq([1, 2, 3, 4, 5, 6])
        end
        it 'raise error when modifying arguments' do
          allow(subject).to receive(:push_4) do |val|
            val.push(4)
          end
          expect { subject.call(:push_4, [1, 2, 3]) }.to raise_error /frozen/
        end
        it 'raise error when modifying instance' do
          expect { subject.call([1, 2, 3], :push, 4) }.to raise_error /frozen/
        end
      end
    end

    describe TCPWorker do
      let(:socket) { double('socket') }
      subject { TCPWorker.new(socket) }

      [:closed?, :close].each do |_alias|
        describe "##{_alias}" do
          it "is an alias for socket.##{_alias}" do
            expect(socket).to receive(_alias).and_return(true)
            expect(subject.send(_alias)).to eq(true)
          end
        end
      end

      describe '#name' do
        it 'returns an identifier for the worker' do
          expect(subject.name).to be_a(String)
        end
      end

      describe '#run_tasks' do
        let(:args) { [:test_method, 1, 2, 3] }
        it 'serialize args, send them through socket, and deserialize result' do
          expect(Marshal).to receive(:dump).with(args, socket).ordered
          expect(Marshal).to receive(:load).with(socket).and_return(2).ordered
          expect(subject.run_task(*args)).to eq(2)
        end

        it 'forwards exception if needed' do
          expect(Marshal).to receive(:dump).with(args, socket).ordered
          expect(Marshal).to receive(:load).with(socket).and_return(
            [nil, RuntimeError.new('test_error')]
          ).ordered
          expect { subject.run_task(*args) }.to raise_error(RuntimeError, /test_error/)
        end
      end
    end

    describe LocalWorker do
      class ContextTest; include Context; end
      let(:context) { ContextTest.new }
      subject { LocalWorker.new(context) }

      describe '#closed?' do
        it 'is always false' do
          expect(subject.closed?).to eq(false)
        end
      end

      it { is_expected.to respond_to(:closed?, :close, :name) }

      describe '#run_task' do
        let(:args) { [:test_method, 1, 2, 3] }
        it 'executes tasks locally using context' do
          expect(context).to receive(:call).with(*args).and_return(4)
          expect(subject.run_task(*args)).to eq(4)
        end
        it 'forwards exceptions occuring in context#call' do
          expect(context).to receive(:call).with(*args) { fail 'boom!' }
          expect { subject.run_task(*args) }.to raise_error('boom!')
        end
      end
    end

    describe TCPWorkerPool do
      let(:tcp_port) { 2000 }
      describe '#listen' do
        it 'starts a tcp server on given port' do
          expect(Thread).to receive(:new) # disable thread creation
          expect(TCPServer).to receive(:new).with(tcp_port)
            .and_return(double('tcp_server'))
          subject.listen(tcp_port)
        end

        it 'fails if already listening' do
          allow(Thread).to receive(:new) # disable thread creation
          expect(TCPServer).to receive(:new).with(tcp_port)
            .and_return(double('tcp_server'))
          subject.listen(tcp_port)
          expect { subject.listen(tcp_port) }.to raise_error /already listening/
        end

        let(:incoming) { 3.times.map { |i| double("socket#{i}") } }
        let(:tcp_server) { double('tcp_server') }
        it "waits for connections,\n" \
          "create a worker for each connection,\n" \
          "close each connection when stopped\n" do
          expect(TCPServer).to receive(:new).with(tcp_port)
            .and_return(tcp_server)
          allow(Thread).to receive(:new).and_call_original

          incoming.each do |socket|
            expect(subject).to receive(:tcp_accept).and_return(socket)
            expect(TCPWorker).to receive(:new).with(socket).and_call_original
          end

          # simulate server shutdown: accept throws a EBADF exception
          expect(subject).to receive(:tcp_accept).and_raise(Errno::EBADF.new)

          incoming.each { |socket| expect(socket).to receive(:close) }

          subject.listen(tcp_port)
          subject.instance_variable_get(:@th_server).join
        end

        # repeat these tests which may have random issues
        REPEAT_COUNT = 50
        class TCPWorkerPool; SELECT_TIMEOUT = 0.001; end # go faster for UT
        it 'can be stopped' do
          REPEAT_COUNT.times do
            begin
              subject = TCPWorkerPool.new
              subject.listen(tcp_port)
            ensure
              expect_block_to_be_done_in(1) { subject.stop! }
            end
          end
        end

        it 'can be stopped when a client is connected' do
          REPEAT_COUNT.times do
            begin
              subject = TCPWorkerPool.new
              subject.listen(tcp_port)
              client_th = Thread.new(TCPSocket.new('localhost', tcp_port)) do |c|
                begin
                  c.read
                rescue Errno::ECONNRESET
                end
              end
            ensure
              expect_block_to_be_done_in(1) do
                subject.stop!
                client_th.join
              end
            end
          end
        end
      end

      describe '#proc' do
        let(:call_args) { [1, :+, 2] }
        subject { TCPWorkerPool.new }
        it 'returns a Proc' do
          expect(Proc).to receive(:new).and_call_original
          f = subject.proc(*call_args)
          expect(f).to be_a Proc
        end
        context 'when a worker is available' do
          let(:worker) { double('worker', name: 'fake_worker', closed?: false) }
          subject { TCPWorkerPool.new([worker]) }
          it 'acquires this worker and release it' do
            expect(subject).to receive(:acquire_worker).and_call_original.ordered
            expect(worker).to receive(:run_task).with(*call_args).and_return(3).ordered
            expect(subject).to receive(:release_worker).and_call_original.ordered
            expect(subject.proc(*call_args).call).to eq(3)
          end

          it 'can receive arguments' do
            extra_args = [1, 2]
            expect(worker).to receive(:run_task).with(*(call_args + extra_args)).and_return(3) 
            expect(subject.proc(*call_args).call(*extra_args)).to eq(3)
          end

          context 'when worker#run_task raise an error' do
            it 'is raised by the future' do
              expect(worker).to receive(:run_task).with(*call_args) { fail 'boom!' }
              expect { subject.proc(*call_args).call }.to raise_error 'boom!'
            end
          end
        end
        it 'close worker and raise an erreur if the client is disconnected' do
          REPEAT_COUNT.times do
            begin
              subject = TCPWorkerPool.new
              subject.listen(tcp_port)
              client_socket = TCPSocket.new('localhost', tcp_port)
              
              # make sure that connection is accepted by server
              worker = subject.send(:acquire_worker)
              expect(worker).not_to be_nil
              subject.send(:release_worker, worker)

              # close socket at client side
              client_socket.close

              expect(worker).to receive(:close).and_call_original
              expect { subject.proc(*call_args).call }.to raise_error do |e|
                expect(e).to be_a(EOFError) | be_a(Errno::ECONNABORTED)
              end

              # worker is removed from available workers
              expect(subject.instance_variable_get(:@workers)).to be_empty
            ensure
              expect_block_to_be_done_in(1) { subject.stop! }
            end
          end
        end
      end
    end

    describe TCPClient do
      let(:host) { 'localhost' }
      let(:tcp_port) { 2000 }
      let(:context) { double('context') }
      let(:socket) { double('socket') }
      subject do
        allow(TCPSocket).to receive(:new).and_return(socket)
        TCPClient.new(host, tcp_port, context)
      end
      describe '#initialize' do
        it 'connects to specified target' do
          expect(TCPSocket).to receive(:new).with(host, tcp_port)
          subject
        end
      end
      describe '#run_task' do
        let(:args) { [1, :+, 2] }
        it 'executes tasks using context and returns a pair [result, exception]' do
          expect(context).to receive(:call).with(*args).and_return(4)
          expect(subject.run_task(args)).to eq([4, nil])
        end
        it 'rescue any non fatal exception' do
          expect(context).to receive(:call).with(*args){ fail 'boom!' }
          expect(subject.run_task(args)).to match_array([nil, kind_of(RuntimeError)])
        end
      end
      describe '#run' do
        let(:task) { [[3, 1, 4], :sort] }
        let(:result) { double('result') }
        it 'read tasks, execute them and send result' do
          # close socket after one call
          allow(socket).to receive(:closed?).and_return(false, true)
          allow(socket).to receive(:eof?).and_return(false, true)
          allow(socket).to receive(:close)

          expect(Marshal).to receive(:load).with(socket).and_return(task)
          expect(subject).to receive(:run_task).with(task).and_return(result)
          expect(Marshal).to receive(:dump).with(result, socket)
          subject.run
        end
        it 'stops when the server closes the connection before accept' do
          REPEAT_COUNT.times do |i|
            server = TCPServer.new(tcp_port)
            th_client = Thread.new(TCPClient.new(host, tcp_port, context)) do |client|
              client.run
            end
            server.close
            th_client.join
          end
        end
        it 'stops when the server closes an established connection' do
          REPEAT_COUNT.times do |i|
            server = TCPServer.new(tcp_port)
            th_client = Thread.new(TCPClient.new(host, tcp_port, context)) do |client|
              client.run
            end
            client_socket = server.accept
            client_socket.close
            th_client.join
            server.close
          end
        end
      end
    end
  end
end

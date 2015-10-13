require 'concurrent-ruby-tcp'

module Concurrent
  module Edge
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

      describe '#future' do
        context 'given a \'standard\' ruby-concurrent executor' do
          let(:executor) { :io }
          it 'creates a regular future which calls methods in the current context' do
            expect(subject).to receive(:test_method).with(1, 2, 3).and_return(4)
            expect(subject.future(executor, :test_method, 1, 2, 3).value!).to eq(4)
          end
        end
        context 'given a TCPWorkerPool executor' do
          let(:executor) { TCPWorkerPool.new }
          it 'creates the future using this executor' do
            expect(executor).to receive(:future).with(:test_method, 1, 2, 3)
            subject.future(executor, :test_method, 1, 2, 3)
          end
        end
      end
    end

    describe TCPWorker do
      let(:socket) { double('socket') }
      subject { TCPWorker.new(socket) }

      [:closed?, :close].each do |_alias|
        describe "##{_alias}" do
          it "is an alias for #socket.#{_alias}" do
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
            subject = TCPWorkerPool.new
            subject.listen(tcp_port)
            subject.stop!
          end
        end

        it 'can be stopped when a client is connected' do
          REPEAT_COUNT.times do
            subject = TCPWorkerPool.new
            subject.listen(tcp_port)
            client_th = Thread.new(TCPSocket.new('localhost', tcp_port)) do |c|
              begin
                c.read
              rescue Errno::ECONNRESET
              end
            end
            subject.stop!
            client_th.join
          end
        end
      end

      describe '#future' do
        describe 'the returned value' do
          let(:executor) { :fast }
          let(:call_args) { [1, :+, 2] }
          subject { TCPWorkerPool.new(executor) }
          it 'is a Concurrent::Future build with the specified executor' do
            expect(Concurrent).to receive(:future).with(executor).and_call_original
            f = subject.future(*call_args)
            expect(f).to be_a Concurrent::Edge::Future
          end
          context 'when a worker is available' do
            let(:worker) { double('worker', name: 'fake_worker', closed?: false) }
            subject { TCPWorkerPool.new(executor, [worker]) }
            it 'acquires this worker and release it' do
              expect(subject).to receive(:acquire_worker).and_call_original.ordered
              expect(worker).to receive(:run_task).with(*call_args).and_return(3).ordered
              expect(subject).to receive(:release_worker).and_call_original.ordered

              expect(subject.future(*call_args).value!).to eq(3)
            end

            context 'when worker#run_task raise an error' do
              it 'is raised by the future' do
                expect(worker).to receive(:run_task).with(*call_args) { fail 'boom!' }
                expect { subject.future(*call_args).value! }.to raise_error 'boom!'
              end
            end
          end
        end
      end
    end
  end
end

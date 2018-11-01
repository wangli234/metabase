import React from "react";
import { Box, Flex } from "grid-styled";
import { Motion, spring } from "react-motion";
import cx from "classnames";
import { findDOMNode } from "react-dom";

import fitViewport from "metabase/hoc/FitViewPort";
import QuestionAndResultLoader from "metabase/containers/QuestionAndResultLoader";
import Visualization from "metabase/visualizations/components/Visualization";

import GuiQueryEditor from "metabase/query_builder/components/GuiQueryEditor";

import { Absolute, Relative } from "metabase/components/Position";
import Button from "metabase/components/Button";
import Card from "metabase/components/Card";
import Icon, { IconWrapper } from "metabase/components/Icon";

const HeaderControls = ({ onToggleReference, onToggleVisualization }) => (
  <Flex align="center">
    <Motion
      defaultStyle={{ scale: 0 }}
      style={{ scale: onToggleVisualization ? spring(1) : spring(0) }}
    >
      {({ scale }) => (
        <Button
          medium
          mx={2}
          primary
          onClick={() => onToggleVisualization()}
          style={{ transform: `scale(${scale})` }}
        >
          Visualize
        </Button>
      )}
    </Motion>
    <IconWrapper onClick={() => onToggleReference()}>
      <Icon name="reference" />
    </IconWrapper>
  </Flex>
);

const PanelHeader = ({ children }) => <h3>{children}</h3>;

const PageWrapper = ({ children }) => (
  <Box p={2} className="relative">
    {children}
  </Box>
);

class ReferencePanel extends React.Component {
  render() {
    return (
      <Card p={2} style={{ width: 320, minHeight: 300 }}>
        <PanelHeader>Reference</PanelHeader>
      </Card>
    );
  }
}

class VisualizationPanel extends React.Component {
  render() {
    return (
      <Card p={2} style={{ width: 320, height: "100%" }}>
        <PanelHeader>Viz settings</PanelHeader>
        <Button primary medium onClick={() => this.props.onClosePanel()}>
          Done
        </Button>
      </Card>
    );
  }
}

class ResultPanel extends React.Component {
  render() {
    return (
      <Box className="border-top bg-white overflow-hidden full-height">
        <Flex
          align="center"
          py={2}
          justify="center"
          w={"100%"}
          className="text-brand-hover cursor-pointer"
        >
          <Icon name="table" mt={1} />
        </Flex>
        <Box
          className="relative"
          style={{ height: "40%", width: "100%" }}
          mx={3}
        >
          <QuestionAndResultLoader questionId={this.props.dataId}>
            {({ question, result, cancel, reload, rawSeries, loading }) => {
              return (
                <Box className="spread flex flex-column">
                  {rawSeries && <Visualization rawSeries={rawSeries} />}
                </Box>
              );
            }}
          </QuestionAndResultLoader>
        </Box>
        <Box
          className="relative"
          style={{ height: "40%", width: "100%" }}
          mx={3}
          mb={3}
        >
          <QuestionAndResultLoader questionId={this.props.aggregationId}>
            {({ question, result, cancel, reload, rawSeries, loading }) => {
              if (!question) {
                return <div>"Loading..."</div>;
              }
              return (
                <Box className="spread flex flex-column">
                  {rawSeries && <Visualization rawSeries={rawSeries} />}
                </Box>
              );
            }}
          </QuestionAndResultLoader>
        </Box>
      </Box>
    );
  }
}

class VisualiztionControls extends React.Component {
  render() {
    return (
      <Flex
        align="center"
        justify="center"
        flexDirection="column"
        style={{ height: "100%" }}
      >
        <Card
          style={{ borderRadius: 10000, lineHeight: 1 }}
          p={1}
          mb={1}
          className="text-brand"
        >
          <Icon name="bar" />
        </Card>
        <Card style={{ borderRadius: 10000, lineHeight: 1 }} p={1} mb={1}>
          <Icon name="line" />
        </Card>
        <Card
          style={{ borderRadius: 10000, lineHeight: 1 }}
          p={1}
          mb={1}
          className="text-brand-hover"
          onClick={() => this.props.onOpenPanel()}
        >
          <Icon name="chevronright" />
        </Card>
      </Flex>
    );
  }
}

@fitViewport
class ModeContainer extends React.Component {
  state = {
    vizPanelOpen: false,
    referencePanelOpen: false,
    showResultPane: false,
  };
  render() {
    const { referencePanelOpen, vizPanelOpen, showResultPane } = this.state;
    const wrapperHeight =
      findDOMNode(this.container) && findDOMNode(this.container).offsetHeight;
    return (
      <Flex
        flexDirection="column"
        w={"100%"}
        className={cx(this.props.fitClassNames, "overflow-hidden")}
      >
        <QuestionAndResultLoader questionId={this.props.params.q1}>
          {({ question, result, cancel, reload, rawSeries, loading }) => {
            if (!question) {
              return <div>"Loading..."</div>;
            }
            window.q = question;
            return (
              <Box className="flex flex-column flex-full">
                <Flex align="center" p={2}>
                  <h2>{question.displayName()}</h2>
                  <Box ml="auto">
                    <HeaderControls
                      onToggleReference={() =>
                        this.setState({
                          referencePanelOpen: !this.state.referencePanelOpen,
                        })
                      }
                      onToggleVisualization={
                        this.state.showResultPane
                          ? () => this.setState({ showResultPane: false })
                          : null
                      }
                    />
                  </Box>
                </Flex>
                <Box
                  className="flex-full relative"
                  ref={node => (this.container = node)}
                >
                  <Motion
                    defaultStyle={{ x: 0, y: 0, opacity: 0 }}
                    style={{
                      x: vizPanelOpen ? spring(40) : spring(0),
                      y: spring(10),
                      opacity: vizPanelOpen ? spring(1) : spring(0),
                    }}
                  >
                    {({ x, y, opacity }) => (
                      <Absolute
                        top={20}
                        bottom={80}
                        style={{
                          opacity,
                          zIndex: 2,
                          transform: `translate3d(${x}px, 0px, 0px)`,
                        }}
                      >
                        <VisualizationPanel
                          onClosePanel={() =>
                            this.setState({ vizPanelOpen: false })
                          }
                        />
                      </Absolute>
                    )}
                  </Motion>
                  <Motion
                    defaultStyle={{ x: 0, y: 0, opacity: 0 }}
                    style={{
                      x: referencePanelOpen ? spring(40) : spring(0),
                      y: spring(10),
                      opacity: referencePanelOpen ? spring(1) : spring(0),
                    }}
                  >
                    {({ x, y, opacity }) => (
                      <Absolute
                        right={0}
                        style={{
                          transform: `translate3d(-${x}px, ${y}px, 0px)`,
                          opacity,
                          zIndex: 4,
                        }}
                      >
                        <ReferencePanel />
                      </Absolute>
                    )}
                  </Motion>
                  <Motion
                    defaultStyle={{ left: 40, right: 0 }}
                    style={{
                      left: vizPanelOpen ? spring(400) : spring(40),
                      right: referencePanelOpen ? spring(360) : spring(0),
                    }}
                  >
                    {({ left, right }) => (
                      <Box style={{ height: "80vh" }} mr={right} ml={left}>
                        {rawSeries && <Visualization rawSeries={rawSeries} />}
                      </Box>
                    )}
                  </Motion>
                  <Absolute top={0} bottom={40} style={{ zIndex: 4 }}>
                    <Flex
                      align="center"
                      justify="center"
                      flexDirection="column"
                    >
                      <VisualiztionControls
                        onOpenPanel={() =>
                          this.setState({ vizPanelOpen: true })
                        }
                      />
                    </Flex>
                  </Absolute>
                  <Motion
                    defaultStyle={{ y: wrapperHeight - 60 }}
                    style={{
                      y: showResultPane
                        ? spring(0)
                        : spring(wrapperHeight - 60),
                    }}
                  >
                    {({ y }) => (
                      <Absolute
                        bottom={0}
                        left={0}
                        right={0}
                        top={0}
                        onClick={() =>
                          this.setState({
                            showResultPane: !this.state.showResultPane,
                          })
                        }
                        style={{
                          zIndex: 3,
                          backgroundColor: "white",
                          transform: `translate3d(0px, ${y}px, 0px)`,
                        }}
                      >
                        <ResultPanel
                          dataId={this.props.params.q2}
                          aggregationId={this.props.params.q3}
                        />
                      </Absolute>
                    )}
                  </Motion>
                </Box>
              </Box>
            );
          }}
        </QuestionAndResultLoader>
      </Flex>
    );
  }
}

export default ModeContainer;
